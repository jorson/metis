package com.huayu.metis.storm.tool;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Lists;
import com.huayu.metis.storm.ConstansValues;
import com.huayu.metis.storm.spout.ApiCallDocument;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.mockito.Mockito.*;

/**
 * Created by Administrator on 14-5-4.
 */
public class MockTupleHelpers {

    private static final String[] apiurls = new String[] {
            "/v1/user/login",
            "/v1/user/get",
            "/v2/user/register",
            "/v1/oauth/verify",
            "/v1/oauth/get",
            "/v1/user/info"
    };
    private static final int[] siteId = new int[] {99, 98, 97, 96, 95, 94};
    private static final int[] statusCode = new int[] {404, 403, 500, 501, 503, 200};
    private static final int[] appId = new int[] {7, 19, 20, 35, 17, 19};
    private static final int[] ipAddress = new int[] {2137059402, 2137058783, 2137058869, 2137058806, 2130706433, 2137059361};
    private static final int[] clientId = new int[] {1001, 1002, 3001, 3004, 4001, 5001};
    private static Random random = new Random();

    private MockTupleHelpers(){

    }

    public static Tuple mockTuple() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValues()).thenReturn(getEnumlateValues());
        when(tuple.getFields()).thenReturn(getEnumlateFields());
        return tuple;
    }

    public static  Tuple mockGroupedTuple() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField(ConstansValues.API_MONITOR_GROUPING_TIMESTAMP_FILED)).thenReturn(new DateTime().getMillis());
        when(tuple.getValueByField(ConstansValues.API_MONITOR_GROUPING_VALUE_FILED)).thenReturn(getGroupedValue());
        when(tuple.getValueByField(ConstansValues.API_MONITOR_GROUPING_KEY_FILED)).thenReturn(getGroupedKey());
        return tuple;
    }

    private static List<Object> getEnumlateValues() {
        int randomNum;
        int docCount = random.nextInt(50);

        List<Object> tuples = Lists.newArrayList();
        DateTime now = new DateTime().toDateTime();

        for(int i=0; i<docCount; i++){
            randomNum = random.nextInt(6);

            ApiCallDocument document = new ApiCallDocument(
                    siteId[randomNum],
                    appId[randomNum],
                    clientId[randomNum],
                    statusCode[randomNum],
                    random.nextInt(200),
                    random.nextInt(300),
                    random.nextInt(500),
                    now.getMillis(),
                    ipAddress[randomNum],
                    apiurls[randomNum]);

            tuples.add(document);

            if(i % 2 == 0) {
                ApiCallDocument document2 = new ApiCallDocument(
                        siteId[randomNum],
                        appId[randomNum],
                        clientId[randomNum],
                        statusCode[randomNum],
                        random.nextInt(200),
                        random.nextInt(300),
                        random.nextInt(500),
                        now.getMillis(),
                        ipAddress[randomNum],
                        apiurls[randomNum]);

                tuples.add(document2);
            }
        }
        return tuples;
    }

    public static Fields getEnumlateFields() {
        return new Fields(ConstansValues.API_MONITOR_TIME_BASE_OUTPUT_KEY);
    }

    private static List<Map<String, Object>> getGroupedValue() {
        int randomNum;
        int docCount = random.nextInt(50);

        List<Map<String, Object>> tuples = Lists.newArrayList();
        DateTime now = new DateTime().toDateTime();

        for(int i=0; i<docCount; i++){
            randomNum = random.nextInt(6);
            ApiCallDocument document = new ApiCallDocument(
                    siteId[randomNum],
                    appId[randomNum],
                    clientId[randomNum],
                    statusCode[randomNum],
                    random.nextInt(200),
                    random.nextInt(300),
                    random.nextInt(500),
                    now.getMillis(),
                    ipAddress[randomNum],
                    apiurls[randomNum]);

            tuples.add(document.getValue());
        }
        return tuples;
    }

    private static Object getGroupedKey() {
        int randomNum;
        DateTime now = new DateTime().toDateTime();
        randomNum = random.nextInt(6);

        ApiCallDocument document = new ApiCallDocument(
                siteId[randomNum],
                appId[randomNum],
                clientId[randomNum],
                statusCode[randomNum],
                random.nextInt(200),
                random.nextInt(300),
                random.nextInt(500),
                now.getMillis(),
                ipAddress[randomNum],
                apiurls[randomNum]);

        return document.getKey();
    }
}
