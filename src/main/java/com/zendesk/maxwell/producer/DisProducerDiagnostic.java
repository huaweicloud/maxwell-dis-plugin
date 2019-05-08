package com.zendesk.maxwell.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.huaweicloud.dis.adapter.kafka.clients.producer.Callback;
import com.huaweicloud.dis.adapter.kafka.clients.producer.ProducerRecord;
import com.huaweicloud.dis.adapter.kafka.clients.producer.RecordMetadata;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.monitoring.MaxwellDiagnosticResult;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.PositionStoreThread;

public class DisProducerDiagnostic implements MaxwellDiagnostic {

	private final MaxwellDisProducerWorker producer;
	private final MaxwellConfig config;
	private final PositionStoreThread positionStoreThread;

	public DisProducerDiagnostic(MaxwellDisProducerWorker producer, MaxwellConfig config, PositionStoreThread positionStoreThread) {
		this.producer = producer;
		this.config = config;
		this.positionStoreThread = positionStoreThread;
	}

	@Override
	public String getName() {
		return "dis-producer";
	}

	@Override
	public CompletableFuture<MaxwellDiagnosticResult.Check> check() {
		return getLatency().thenApply(this::normalResult).exceptionally(this::exceptionResult);
	}

	@Override
	public boolean isMandatory() {
		return true;
	}

	@Override
	public String getResource() {
		return config.getDisProperties().getProperty("endpoint");
	}

	public CompletableFuture<Long> getLatency() {
		DiagnosticCallback callback = new DiagnosticCallback();
		try {
			RowMap rowMap = new RowMap("insert", config.databaseName, "dummy", System.currentTimeMillis(),
					new ArrayList<>(), positionStoreThread.getPosition());
			rowMap.setTXCommit();
			ProducerRecord<String, String> record = producer.makeProducerRecord(rowMap);
			producer.sendAsync(record, callback);
		} catch (Exception e) {
			callback.latency.completeExceptionally(e);
		}
		return callback.latency;
	}

	private MaxwellDiagnosticResult.Check normalResult(Long latency) {
		Map<String, String> info = new HashMap<>();
		info.put("message", "Kafka producer acknowledgement lag is " + latency.toString() + "ms");
		return new MaxwellDiagnosticResult.Check(this, true, info);
	}

	private MaxwellDiagnosticResult.Check exceptionResult(Throwable e) {
		Map<String, String> info = new HashMap<>();
		info.put("error", e.getCause().toString());
		return new MaxwellDiagnosticResult.Check(this, false, info);
	}

	static class DiagnosticCallback implements Callback {
		final CompletableFuture<Long> latency;
		final long sendTime;

		DiagnosticCallback() {
			latency = new CompletableFuture<>();
			sendTime = System.currentTimeMillis();
		}

		@Override
		public void onCompletion(final RecordMetadata metadata, final Exception exception) {
			if (exception == null) {
				latency.complete(System.currentTimeMillis() - sendTime);
			} else {
				latency.completeExceptionally(exception);
			}
		}
	}
}
