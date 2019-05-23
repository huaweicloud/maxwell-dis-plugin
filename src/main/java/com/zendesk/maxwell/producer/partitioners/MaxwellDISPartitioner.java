package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

public class MaxwellDISPartitioner extends AbstractMaxwellPartitioner {
	HashFunction hashFunc;

	public MaxwellDISPartitioner(String hashFunction, String partitionKey, String csvPartitionColumns, String partitionKeyFallback) {
		super(partitionKey, csvPartitionColumns, partitionKeyFallback);

		int MURMUR_HASH_SEED = 25342;
		switch (hashFunction) {
			case "murmur3": this.hashFunc = new HashFunctionMurmur3(MURMUR_HASH_SEED);
				break;
			case "default":
			default:
				this.hashFunc = new HashFunctionDefault();
				break;
		}
	}

	public int disPartition(RowMap r, int numPartitions) {
		return Math.abs(hashFunc.hashCode(this.getHashString(r)) % numPartitions);
	}

	public int disPartition(RowMap r, String key, int numPartitions) {
		if (partitionBy == PartitionBy.PARTITION_KEY) {
			return Math.abs(hashFunc.hashCode(key) % numPartitions);
		} else {
			return disPartition(r, numPartitions);
		}
	}
}
