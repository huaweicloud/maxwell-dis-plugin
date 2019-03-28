package com.zendesk.maxwell.producer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.sdk.adapter.kafka.producer.DISKafkaProducer;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellKafkaPartitioner;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMap.KeyFormat;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;

class DisCallback implements Callback {
	public static final Logger LOGGER = LoggerFactory.getLogger(MaxwellDisProducer.class);
	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private final String key;
	private final MaxwellContext context;

	private Counter succeededMessageCount;
	private Counter failedMessageCount;
	private Meter succeededMessageMeter;
	private Meter failedMessageMeter;

	public DisCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, String key, String json,
	                     Counter producedMessageCount, Counter failedMessageCount, Meter producedMessageMeter,
	                     Meter failedMessageMeter, MaxwellContext context) {
		this.cc = cc;
		this.position = position;
		this.key = key;
		this.json = json;
		this.succeededMessageCount = producedMessageCount;
		this.failedMessageCount = failedMessageCount;
		this.succeededMessageMeter = producedMessageMeter;
		this.failedMessageMeter = failedMessageMeter;
		this.context = context;
	}

	@Override
	public void onCompletion(RecordMetadata md, Exception e) {
		if ( e != null ) {
			this.failedMessageCount.inc();
			this.failedMessageMeter.mark();

			LOGGER.error(e.getClass().getSimpleName() + " @ " + position + " -- " + key);
			LOGGER.error(e.getLocalizedMessage());
			if ( e instanceof RecordTooLargeException ) {
				LOGGER.error("Considering raising max.request.size broker-side.");
			}
			else if (e instanceof DISClientException && e.getMessage().contains("DIS.4213")) {
				LOGGER.error(e.getMessage(), e);
			}
			else if (!this.context.getConfig().ignoreProducerError) {
				this.context.terminate(e);
				return;
			}
		} else {
			this.succeededMessageCount.inc();
			this.succeededMessageMeter.mark();

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("->  partitionKey:" + key + ", partitionId:" + md.partition() + ", sequenceNumber:" + md.offset());
				LOGGER.debug("   " + this.json);
				LOGGER.debug("   " + position);
				LOGGER.debug("");
			}
		}

		cc.markCompleted();
	}
}

public class MaxwellDisProducer extends AbstractProducer {
	private final ArrayBlockingQueue<RowMap> queue;
	private final MaxwellDisProducerWorker worker;

	public MaxwellDisProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic) {
		super(context);
		this.queue = new ArrayBlockingQueue<>(100);
		this.worker = new MaxwellDisProducerWorker(context, kafkaProperties, kafkaTopic, this.queue);
		Thread thread = new Thread(this.worker, "maxwell-dis-worker");
		thread.setDaemon(true);
		thread.start();
	}

	@Override
	public void push(RowMap r) throws Exception {
		this.queue.put(r);
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this.worker;
	}

	@Override
	public DisProducerDiagnostic getDiagnostic() {
		return new DisProducerDiagnostic(worker, context.getConfig(), context.getPositionStoreThread());
	}
}

class MaxwellDisProducerWorker extends AbstractAsyncProducer implements Runnable, StoppableTask {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellDisProducer.class);

	private final Producer<String, String> kafka;
	private String topic;
	private final String ddlTopic;
	private final MaxwellKafkaPartitioner partitioner;
	private final MaxwellKafkaPartitioner ddlPartitioner;
	private final KeyFormat keyFormat;
	private final boolean interpolateTopic;
	private final ArrayBlockingQueue<RowMap> queue;
	private Thread thread;
	private StoppableTaskState taskState;
	private final Map<String, PartitionNum> partitionNumMap = new ConcurrentHashMap<>();
	
	public static MaxwellKafkaPartitioner makeDDLPartitioner(String partitionHashFunc, String partitionKey) {
		if ( partitionKey.equals("table") ) {
			return new MaxwellKafkaPartitioner(partitionHashFunc, "table", null, "database");
		} else {
			return new MaxwellKafkaPartitioner(partitionHashFunc, "database", null, null);
		}
	}
	
	public MaxwellDisProducerWorker(MaxwellContext context, Properties kafkaProperties, String kafkaTopic, ArrayBlockingQueue<RowMap> queue) {
		super(context);

		this.topic = kafkaTopic;
		Preconditions.checkArgument(topic != null, "dis.stream is null");

		this.interpolateTopic = this.topic.contains("%{");
		this.kafka = new DISKafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());

		String hash = context.getConfig().disPartitionHash;
		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		this.partitioner = new MaxwellKafkaPartitioner(hash, partitionKey, partitionColumns, partitionFallback);

		this.ddlPartitioner = makeDDLPartitioner(hash, partitionKey);
		this.ddlTopic =  context.getConfig().disDdlStream;

		if ( context.getConfig().disKeyFormat.equals("hash") )
			keyFormat = KeyFormat.HASH;
		else
			keyFormat = KeyFormat.ARRAY;

		this.queue = queue;
		this.taskState = new StoppableTaskState("MaxwellDisProducerWorker");
		this.partitionNumMap.put(kafkaTopic, new PartitionNum(kafka.partitionsFor(kafkaTopic).size()));
		if (StringUtils.isNotBlank(ddlTopic) && !ddlTopic.equals(kafkaTopic))
		{
			this.partitionNumMap.put(ddlTopic, new PartitionNum(kafka.partitionsFor(ddlTopic).size()));
		}
	}

	@Override
	public void run() {
		this.thread = Thread.currentThread();
		while ( true ) {
			try {
				RowMap row = queue.take();
				if (!taskState.isRunning()) {
					taskState.stopped();
					return;
				}
				this.push(row);
			} catch ( Exception e ) {
				taskState.stopped();
				context.terminate(e);
				return;
			}
		}
	}

	private Integer getNumPartitions(String topic)
	{
		PartitionNum partitionNum = this.partitionNumMap.get(topic);
		if (partitionNum == null || partitionNum.expire(context.getConfig().disStreamCacheSecond))
		{
			partitionNum = new PartitionNum(kafka.partitionsFor(topic).size());
			LOGGER.info("Refresh stream {}, partitionNum is {}", topic, partitionNum.partitionNum);
			this.partitionNumMap.put(topic, partitionNum);
		}
		return partitionNum.partitionNum;
	}

	private String generateTopic(String topic, RowMap r){
		if ( interpolateTopic )
			return topic.replaceAll("%\\{database\\}", r.getDatabase()).replaceAll("%\\{table\\}", r.getTable());
		else
			return topic;
	}

	@Override
	public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
		ProducerRecord<String, String> record = makeProducerRecord(r);

		/* if debug logging isn't enabled, release the reference to `value`, which can ease memory pressure somewhat */
		String value = DisCallback.LOGGER.isDebugEnabled() ? record.value() : null;

		DisCallback callback = new DisCallback(cc, r.getNextPosition(), record.key(), value,
				this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter, this.context);

		sendAsync(record, callback);
	}

	void sendAsync(ProducerRecord<String, String> record, Callback callback) throws Exception {
		kafka.send(record, callback);
	}

	ProducerRecord<String, String> makeProducerRecord(final RowMap r) throws Exception {
		String key = r.pkToJson(keyFormat);
		String value = r.toJSON(outputConfig);
		ProducerRecord<String, String> record;
		if (r instanceof DDLMap) {
			record = new ProducerRecord<>(this.ddlTopic, this.ddlPartitioner.kafkaPartition(r, getNumPartitions(this.ddlTopic)), key, value);
		} else {
			String topic;

			// javascript topic override
			topic = r.getKafkaTopic();
			if ( topic == null )
				topic = generateTopic(this.topic, r);

			record = new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, getNumPartitions(topic)), key, value);
		}
		return record;
	}

	@Override
	public void requestStop() {
		taskState.requestStop();
		// TODO: set a timeout once we drop support for kafka 0.8
		kafka.close();
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {
		taskState.awaitStop(thread, timeout);
	}

	// force-close for tests.
	public void close() {
		kafka.close();
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}
	
	public class PartitionNum
	{
		private Integer partitionNum;

		private long createTimestamp;

		public PartitionNum(Integer partitionNum)
		{
			this.partitionNum = partitionNum;
			this.createTimestamp = System.currentTimeMillis();
		}

		public boolean expire(long cacheSecond)
		{
			return cacheSecond >= 0 && (System.currentTimeMillis() - this.createTimestamp) > cacheSecond * 1000;
		}
	}
}
