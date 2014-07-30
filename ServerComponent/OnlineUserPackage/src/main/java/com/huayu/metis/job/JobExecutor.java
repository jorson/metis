package com.huayu.metis.job;

/**
 * M/R 作业运行期
 * Created by Administrator on 14-7-30.
 */
public class JobExecutor {

    public static final String COMBINE_FILE_JOB_KEY = "combine.file";
    public static final String USER_PAGE_VISIT_JOB_KEY = "user.page.visit";
    public static final String PAGE_VISIT_JOB_KEY = "page.visit";

    private static String[] jobArgs;

    /**
     * 执行JOB的函数
     * @param args 执行参数,至少2个. 执行JOB的Key和配置文件的路径
     */
    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("please enter the key of job and config file path! example: JobExecutor combine.file conf/config.properties");
            return;
        }

        String jobKey = args[0];
        String configFilePath = args[1];
        BasicJob job = null;

        if(args.length > 2) {
            jobArgs = new String[args.length - 2];
            for(int i=2; i<args.length;  i++) {
                jobArgs[i - 2] = args[i];
            }
        }

        if(jobKey.equalsIgnoreCase(COMBINE_FILE_JOB_KEY)) {
            job = new CombineFileJob(configFilePath);
        } else if(jobKey.equalsIgnoreCase(USER_PAGE_VISIT_JOB_KEY)) {
            job = new UserPageVisitJob(configFilePath);
        } else if(jobKey.equalsIgnoreCase(PAGE_VISIT_JOB_KEY)) {
            job = new PageVisitJob(configFilePath);
        }

        if(job != null) {
            job.runJob(jobArgs);
        } else {
            throw new IllegalArgumentException("unsupported job key");
        }

    }
}
