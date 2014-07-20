package com.huayu.metis.storm.bolt.mysql;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.huayu.metis.storm.config.ApiMonitorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 14-4-30.
 */
public abstract class MySQLBoltBase extends BaseRichBolt {

    private static Logger logger = LoggerFactory.getLogger(MySQLBoltBase.class);

    //Bolt的运行时对象
    protected Map conf;
    protected TopologyContext context;
    protected OutputCollector collector;

    protected String url;
    protected String userName;
    protected String password;
    protected String tableName;
    protected String[] columns;

    private String columnString;

    //JDBC object
    protected Connection connection;


    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.context = topologyContext;
        this.conf = conf;

        //打开MySQL的数据库连接
        try{
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url, userName, password);
        } catch(Exception ex) {
            logger.error("Connect MySQL error", ex);
        }
        //构建插入数据库的字段字符串
        this.columnString = buildColumns();

    }

    @Override
    public abstract void execute(Tuple tuple);

    @Override
    public abstract void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    @Override
    public abstract void cleanup();

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }

    protected void put(List<Object> values) {
        if(values.size() != columns.length){
            return;
        }

        StringBuilder queryBuilder = new StringBuilder().append("Insert Into ")
                .append(this.tableName)
                .append("(")
                .append(this.columnString)
                .append(") Values ")
                .append("(")
                .append(Joiner.on(",").join(repeat("?", columns.length)))
                .append(")")
                .append(" On Duplicate Key Update ")
                .append(Joiner.on(",").join(Lists.transform(Lists.newArrayList(columns), new Function<String, String>() {
                    @Override
                    public String apply(String col) {
                        return col + " = Values(" + col + ")";
                    }
                })));
        PreparedStatement statement = null;
        try{
            String sql = queryBuilder.toString();
            if(logger.isDebugEnabled()) {
                logger.debug("Execute SQL:" + sql);
            }
            statement = connection.prepareStatement(sql);
            int counter = 0;
            for(Object obj : values) {
                statement.setObject(++counter, obj);
            }
            System.out.println("Execute SQL" + sql);
            statement.execute();
        } catch (SQLException ex) {
            logger.error("put update failed", ex);
        } finally {
            if(statement != null){
                try{
                    statement.close();
                } catch (Exception ex) {

                }
            }
        }
    }

    private String buildColumns() {
        List<String> cols = Lists.newArrayList(columns);
        return Joiner.on(',').join(cols);
    }

    private <U> List<U> repeat(final U val, final int count) {
        final List<U> list = new ArrayList<U>();
        for(int i=0; i<count; i++) {
            list.add(val);
        }
        return list;
    }
}
