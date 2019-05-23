package com.zendesk.maxwell.producer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class TableMapping {
    private List<TableMappingRule> rules = Collections.emptyList();

    public TableMappingRule matches(String database, String tableName) {
        for (TableMappingRule rule : rules) {
            if (rule.matches(database, tableName)) {
                return rule;
            }
        }
        return null;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class TableMappingRule {
        @JsonProperty("database")
        private String database;
        @JsonProperty("table_name")
        private String tableName;
        @JsonProperty("stream_name")
        private String streamName;
        @JsonProperty("partition_id")
        private Integer partitionId;

        private Pattern tableNamePattern;

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTableName() {
            return tableName;
        }

        public boolean matches(String database, String tableName) {
            return this.database.equals(database) && (this.tableName == null || tableNamePattern.matcher(tableName).matches());
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
            if (tableName != null) {
                this.tableNamePattern = Pattern.compile(tableName);
            }
        }

        public String getStreamName() {
            return streamName;
        }

        public void setStreamName(String streamName) {
            this.streamName = streamName;
        }

        public Integer getPartitionId() {
            return partitionId;
        }

        public void setPartitionId(Integer partitionId) {
            this.partitionId = partitionId;
        }
    }

    public List<TableMappingRule> getRules() {
        return rules;
    }

    public void setRules(List<TableMappingRule> rules) {
        this.rules = rules;
    }
}
