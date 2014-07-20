package com.huayu.metis.options;

/**
 * Created by Administrator on 14-5-16.
 */
public enum AfterTaskBehavior {
    //继续执行下一个任务(如果存在)
    ContinueNext,
    //结束并报告成功
    FinishAndReportSuccess,
    //结束并报告失败
    FinishAndReportFailure
}
