package com.huayu.metis.storm.spout;

import com.huayu.metis.storm.spout.mongo.MongoObjectGrabber;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 14-4-30.
 */
public class ApiCallDocument extends MongoObjectGrabber<ApiCallDocument> {

    private int siteId;
    private int clientId;
    private int appId;
    private int statusCode;
    private int responseTime;
    private int requestSize;
    private int responseSize;
    private long logDate;
    private long ipAddress;
    private String callUrl;

    private static final String[] fields = new String[]{
            "_id",
            "siteId",
            "appId",
            "clientId",
            "statusCode",
            "requestSize",
            "responseTime",
            "responseSize",
            "logDate",
            "ipAddress",
            "callUrl"
    };

    public ApiCallDocument() {

    }

    /**
     * API调用原始数据的对象
     * @param siteId API站点ID
     * @param appId 调用接口的APP ID
     * @param clientId 调用客户端编码
     * @param statusCode 请求状态码
     * @param requestSize 请求大小
     * @param responseTime 响应时间
     * @param responseSize 响应大小
     * @param logDate 日志日期
     * @param ipAddress 服务端地址
     * @param callUrl 调用URL
     */
    public ApiCallDocument(int siteId, int appId, int clientId, int statusCode,
                           int requestSize, int responseTime, int responseSize,
                           long logDate, long ipAddress, String callUrl) {
        this.siteId = siteId;
        this.appId = appId;
        this.clientId = clientId;
        this.statusCode = statusCode;
        this.requestSize = requestSize;
        this.responseSize = responseSize;
        this.responseTime = responseTime;
        this.logDate = logDate;
        this.ipAddress = ipAddress;
        this.callUrl = callUrl;
    }

    @Override
    public ApiCallDocument map(DBObject object) {

        ApiCallDocument document = new ApiCallDocument();

        document._id = (ObjectId)object.get(fields[0]);
        document.siteId = Integer.parseInt(object.get(fields[1]).toString());
        document.appId = Integer.parseInt(object.get(fields[2]).toString());
        document.clientId = Integer.parseInt(object.get(fields[3]).toString());
        document.statusCode = Integer.parseInt(object.get(fields[4]).toString());
        document.requestSize = Integer.parseInt(object.get(fields[5]).toString());
        document.responseTime = Integer.parseInt(object.get(fields[6]).toString());
        document.responseSize = Integer.parseInt(object.get(fields[7]).toString());
        document.logDate = Long.parseLong(object.get(fields[8]).toString());
        document.ipAddress = Long.parseLong(object.get(fields[9]).toString());
        document.callUrl = object.get(fields[10]).toString();
        return document;
    }

    @Override
    public String[] fields() {
        return fields;
    }

    @Override
    public Object getKey() {
        return callUrl + "," + String.valueOf(this.siteId) + ","
                + String.valueOf(this.appId) + "," + String.valueOf(this.clientId)
                + "," + String.valueOf(this.statusCode) + "," + String.valueOf(ipAddress);
    }

    @Override
    public Map<String, Object> getValue() {

        Map<String, Object> result = new HashMap<String, Object>();
        result.put("responseTime", this.responseTime);
        result.put("requestSize", this.requestSize);
        result.put("responseSize", this.responseSize);
        return result;
    }

    public int getClientId() {
        return clientId;
    }

    public int getAppId() {
        return appId;
    }

    public int getResponseTime() {
        return responseTime;
    }

    public int getResponseSize() {
        return responseSize;
    }

    public long getLogDate() {
        return logDate;
    }

    public String getCallUrl() {
        return callUrl;
    }

    public long getIpAddress() {
        return ipAddress;
    }

    public int getSiteId() {
        return siteId;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public int getRequestSize() {
        return requestSize;
    }

    public static String[] getFields() {
        return fields;
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof ApiCallDocument)){
            return false;
        }
        ApiCallDocument that = (ApiCallDocument)obj;
        int result = Integer.compare(this.hashCode(), that.hashCode());
        return result == 0;
    }

    @Override
    public int hashCode() {
        return this.clientId +
                this.appId +
                (int)this.ipAddress +
                this.callUrl.hashCode();
    }
}
