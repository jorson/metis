package com.huayu.metis.jobs;

import com.huayu.metis.options.JobAgentOption;
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Created by Administrator on 14-5-29.
 */
public class JobAgentConfigTest {

    @Test
    public void toJsonTest() {
        JobAgentOption option = new JobAgentOption();
        String json = JobAgentOption.serialize(option);
        System.out.println(json);
    }

    @Test
    public void fromJsonTest() throws FileNotFoundException {
        String configPath = Thread.currentThread().getContextClassLoader()
                .getResource("local-job-config.json").getPath();
        File configFile = new File(configPath);
        JobAgentOption option = JobAgentOption.deserializer(configFile);

        Assert.assertEquals(option.getAgentName(), "test-agent");
    }
}
