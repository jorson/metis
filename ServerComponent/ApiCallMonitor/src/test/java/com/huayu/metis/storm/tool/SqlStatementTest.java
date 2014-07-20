package com.huayu.metis.storm.tool;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 14-5-6.
 */
public class SqlStatementTest {

    //JDBC object
    protected static String url;
    protected static String userName;
    protected static String password;
    protected static Connection connection;
    protected static String tableName;
    protected static String[] columns;

    private static String columnString;

    @BeforeClass
    public static void setup() {
        columns =   new String[] {"StopTimestamp","ApiUrl","AppId", "IpAddress","CallTimes","AvgResponseTime","SumResponseSize"};
        tableName = "apicall_statistics";
        url = "jdbc:MySQL://mysql.server.ty.nd/apimonitor_dev";
        userName = "space";
        password = "888888";


        //打开MySQL的数据库连接
        try{
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url, userName, password);
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        //构建插入数据库的字段字符串
        columnString = buildColumns();
    }

    @AfterClass
    public static void cleanup() {
        if(connection != null){
            connection = null;
        }
    }

    @Test
    public void put() {
        List<Object> values = new ArrayList<Object>();
        values.add(1234567);
        values.add("/Test");
        values.add(12);
        values.add(125151);
        values.add(1);
        values.add(12.8);
        values.add(124);


        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("Insert Into ");
        queryBuilder.append(tableName);
        queryBuilder.append("(");
        queryBuilder.append(columnString);
        queryBuilder.append(") Values ");
        queryBuilder.append("(");
        queryBuilder.append(Joiner.on(",").join(repeat("?", columns.length)));
        queryBuilder.append(")");
        queryBuilder.append(" On Duplicate Key Update ");
        queryBuilder.append(Joiner.on(",").join(Lists.transform(Lists.newArrayList(columns), new Function<String, String>() {
            @Override
            public String apply(String col) {
                return col + " = Values(" + col + ")";
            }
        })));
        System.out.println(queryBuilder.toString());
        PreparedStatement statement = null;
        try{
            String sql = queryBuilder.toString();
            statement = connection.prepareStatement(sql);
            int counter = 0;
            for(Object obj : values) {
                statement.setObject(++counter, obj);
            }
            statement.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            if(statement != null){
                try{
                    statement.close();
                } catch (Exception ex) {

                }
            }
        }
    }

    private static String buildColumns() {
        List<String> cols = Lists.newArrayList(columns);
        return Joiner.on(',').join(cols);
    }

    private static <U> List<U> repeat(final U val, final int count) {
        final List<U> list = new ArrayList<U>();
        for(int i=0; i<count; i++) {
            list.add(val);
        }
        return list;
    }
}
