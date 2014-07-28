package com.huayu.metis.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Administrator on 14-7-28.
 */
public class CustomDbOutputFormat <K extends DBWritable, V extends DBWritable> extends DBOutputFormat<K, V> {

    private static final Log LOG = LogFactory.getLog(CustomDbOutputFormat.class);

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
            throws IOException {
        DBConfiguration dbConf = new DBConfiguration(context.getConfiguration());
        String tableName = dbConf.getOutputTableName();
        String[] fieldNames = dbConf.getOutputFieldNames();

        if(fieldNames == null){
            fieldNames = new String[dbConf.getOutputFieldCount()];
        }

        try{
            Connection connection = dbConf.getConnection();
            PreparedStatement statement = null;
            statement = connection.prepareStatement(super.constructQuery(tableName, fieldNames));
            return new CustomDbRecordWriter<K, V>(connection, statement);
        }catch(Exception ex){
            throw new IOException(ex.getMessage());
        }
    }

    public class CustomDbRecordWriter <K extends DBWritable, V extends DBWritable>
            extends RecordWriter<K, V>{

        private Connection connection;
        private PreparedStatement statement;

        public CustomDbRecordWriter() throws SQLException {
            super();
        }

        public CustomDbRecordWriter(Connection connection, PreparedStatement statement) throws SQLException {
            this.connection = connection;
            this.statement = statement;
            this.connection.setAutoCommit(false);
            LOG.debug("User CustomDbRecordWriter");
        }

        @Override
        public void write(K key, V value) throws IOException {
            try{
                key.write(statement);
                value.write(statement);
                statement.addBatch();
            }catch(SQLException e){
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                statement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    LOG.warn(StringUtils.stringifyException(ex));
                }
                throw new IOException(e.getMessage());
            } finally {
                try {
                    statement.close();
                    connection.close();
                } catch (SQLException ex) {
                    throw new IOException(ex.getMessage());
                }
            }
        }
    }

    public static void setOutput(Job job, String tableName, String... fieldNames) throws IOException{
        if (fieldNames.length > 0 && fieldNames[0] != null) {
            DBConfiguration dbConf = setOutput(job, tableName);
            dbConf.setOutputFieldNames(fieldNames);
        } else {
            if (fieldNames.length > 0) {
                setOutput(job, tableName, fieldNames.length);
            } else {
                throw new IllegalArgumentException(
                        "Field names must be greater than 0");
            }
        }
    }

    public static void setOutput(Job job, String tableName, int fieldCount)
            throws IOException {
        DBConfiguration dbConf = setOutput(job, tableName);
        dbConf.setOutputFieldCount(fieldCount);
    }


    private static DBConfiguration setOutput(Job job, String tableName)
            throws IOException {
        job.setOutputFormatClass(CustomDbOutputFormat.class);
        job.setReduceSpeculativeExecution(false);

        DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());

        dbConf.setOutputTableName(tableName);
        return dbConf;
    }
}
