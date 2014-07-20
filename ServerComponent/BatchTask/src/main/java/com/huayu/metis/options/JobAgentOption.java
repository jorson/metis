package com.huayu.metis.options;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Created by Administrator on 14-5-29.
 */
public class JobAgentOption {

    @SerializedName("jobAgent")
    private String agentName;
    @SerializedName("jobScanRollPoling")
    private int scannerRollPoling;
    @SerializedName("jobCatchRollPoling")
    private int catcherRollPoling;
    @SerializedName("jobThreadNum")
    private int maxThreadNum;
    @SerializedName("jobs")
    private List<JobOption> jobOptionList;

    public JobAgentOption() {

    }

    public static JobAgentOption deserializer(String json) {
        Gson gson = new Gson();
        return gson.fromJson(json, JobAgentOption.class);
    }

    public static JobAgentOption deserializer(FileInputStream configStream) {
        try {
            int size = configStream.available();
            byte[] buffer = new byte[size];
            configStream.read(buffer);

            //转换为Stream
            String json = new String(buffer);
            return deserializer(json);

        } catch(IOException e) {
            //TODO: 错误处理
            e.printStackTrace();
        }
        return null;
    }

    public static JobAgentOption deserializer(File file) throws FileNotFoundException {
        if(file == null || !file.exists()) {
            throw new FileNotFoundException("config file not found");
        }
        FileInputStream fis = new FileInputStream(file);
        return deserializer(fis);
    }

    public static String serialize(JobAgentOption option) {
        Gson gson = new Gson();
        Type optionType = new TypeToken<JobAgentOption>(){}.getType();
        return gson.toJson(option, optionType);
    }

    public String getAgentName() {
        return agentName;
    }

    public int getScannerRollPoling() {
        return scannerRollPoling;
    }

    public int getCatcherRollPoling() {
        return catcherRollPoling;
    }

    public int getMaxThreadNum() {
        return maxThreadNum;
    }

    public List<JobOption> getJobOptionList() {
        return jobOptionList;
    }
}
