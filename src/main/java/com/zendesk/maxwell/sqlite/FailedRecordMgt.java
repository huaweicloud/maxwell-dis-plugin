package com.zendesk.maxwell.sqlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;

public class FailedRecordMgt {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailedRecordMgt.class);

    private static final int DEFAULT_DB_CONNECTION_TIMEOUT_SECONDS = 5;

    private static final int DEFAULT_DB_QUERY_TIMEOUT_SECONDS = 30;

    private final Path dbFile;

    private Connection connection;

    private static FailedRecordMgt instance = new FailedRecordMgt();

    public FailedRecordMgt() {
        dbFile = Paths.get("conf" + File.separator + "failed_records.db");
    }

    public static FailedRecordMgt getInstance() {
        return instance;
    }

    private synchronized boolean isConnected() {
        return connection != null;
    }

    public synchronized void close() {
        if (isConnected()) {
            try {
                LOGGER.debug("Closing connection to database {}...", dbFile);
                connection.close();
            } catch (SQLException e) {
                LOGGER.error("Failed to cleanly close the database {}", dbFile, e);
            }
        }
        connection = null;
    }

    private synchronized void connect() {
        if (!isConnected()) {
            try {
                Class.forName("org.sqlite.JDBC");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to load SQLite driver.", e);
            }

            try {
                LOGGER.debug("Connecting to database {}...", dbFile);
                if (!Files.isDirectory(dbFile.getParent())) {
                    Files.createDirectories(dbFile.getParent());
                }
                String connectionString = String.format("jdbc:sqlite:%s", dbFile.toString());
                connection = DriverManager.getConnection(connectionString);
                connection.setAutoCommit(false);
            } catch (SQLException | IOException e) {
                throw new RuntimeException("Failed to create or connect to the checkpoint database.", e);
            }
            try {
                Statement statement = connection.createStatement();
                statement.executeUpdate(
                        "CREATE TABLE if not exists `t_failed_data` (\n" +
                                "`id` VARCHAR(50) NOT NULL,\n" +
                                "`type` VARCHAR(50) NULL,\n" +
                                "`database` VARCHAR(100) NULL,\n" +
                                "`table` VARCHAR(50) NULL,\n" +
                                "`data` TEXT NULL,\n" +
                                "`key` TEXT NULL,\n" +
                                "`position` TEXT NULL,\n" +
                                "`next_position` TEXT NULL,\n" +
                                "`ts` BIGINT NULL,\n" +
                                "`status` TINYINT NULL,\n" +
                                "`create_time` TIMESTAMP NULL,\n" +
                                "PRIMARY KEY (`id`))");
            } catch (SQLException e) {
                throw new RuntimeException("Failed to configure the checkpoint database.", e);
            }
        }
    }

    protected boolean ensureConnected() {
        if (isConnected()) {
            return true;
        } else {
            try {
                connect();
                return true;
            } catch (Exception e) {
                LOGGER.error("Failed to open a connection to the checkpoint database.", e);
                return false;
            }
        }
    }

    public synchronized void insertFailedRecord(FailedRecord failedRecord) {
        if (!ensureConnected()) {
            return;
        }
        try {
            PreparedStatement insert = connection.prepareStatement("insert or ignore into `t_failed_data` " +
                    "(`id`, `type`, `database`, `table`, `data`, `key`, `position`, `next_position`, `ts`, `status`, `create_time`) VALUES" +
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%d %H:%M:%f', 'now'))");
            insert.setString(1, failedRecord.getId());
            insert.setString(2, failedRecord.getType());
            insert.setString(3, failedRecord.getDatabase());
            insert.setString(4, failedRecord.getTable());
            insert.setString(5, failedRecord.getData());
            insert.setString(6, failedRecord.getKey());
            insert.setString(7, failedRecord.getPosition());
            insert.setString(8, failedRecord.getNextPosition());
            insert.setLong(9, failedRecord.getTs());
            insert.setInt(10, failedRecord.getStatus());
            int affected = insert.executeUpdate();
            if (affected == 1) {
                LOGGER.trace("Created new database checkpoint: {}@{}", failedRecord.getDatabase(), failedRecord.getTable());
            } else {
                LOGGER.error("Did not update or create checkpoint because of race condition: {}@{}",
                        failedRecord.getDatabase(), failedRecord.getTable());
                throw new RuntimeException(
                        "Race condition detected when setting checkpoint for:" + failedRecord.getDatabase() + "@" + failedRecord.getTable());
            }

            connection.commit();
        } catch (SQLException e) {
            LOGGER.error("Failed to create the checkpoint {}@{} in database {}", failedRecord.getDatabase(), failedRecord.getTable(), dbFile, e);
            try {
                connection.rollback();
            } catch (SQLException e2) {
                LOGGER.error("Failed to rollback checkpointing transaction: {}@{}", failedRecord.getDatabase(), failedRecord.getTable());
                LOGGER.info("Reinitializing connection to database {}", dbFile);
                close();
            }
        }
    }
}
