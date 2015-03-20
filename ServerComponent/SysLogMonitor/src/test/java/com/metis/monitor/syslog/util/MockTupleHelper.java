package com.metis.monitor.syslog.util;

import backtype.storm.tuple.Tuple;
import com.metis.monitor.syslog.entry.OriginalSysLog;
import com.metis.monitor.syslog.entry.SysLogDetail;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.mockito.Mockito.*;

/**
 * Created by Administrator on 14-8-7.
 */
public class MockTupleHelper {

    private static final int[] appId = new int[] {7, 19, 20, 35, 17};
    private static final int[] logLevel = new int[] {1, 2, 3, 4, 5};
    private static final String[] logMessage = new String[]
            {
                    "LogMessage1",
                    "LogMessage2",
                    "LogMessage3",
                    "LogMessage4",
                    "LogMessage5"
            };
    private static final String[] logCallStack = new String[]
            {
                    "LogCallStack1",
                    "LogCallStack2",
                    "LogCallStack3",
                    "LogCallStack4",
                    "LogCallStack5"
            };
    private static Random random = new Random();
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    @Test
    public static Tuple mockOriginalSysLogTuple() throws ParseException {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField(ConstVariables.SYS_LOG_ORIGINAL_VALUE_FILED))
                .thenReturn(getRandomOriginalSysLog());
        return tuple;
    }

    public static Tuple mockOriginalInput() {
        List<Object> temp = new ArrayList<Object>();
        temp.add("10%094%09lesson%E5%8F%82%E6%95%B0%E9%94%99%E8%AF%AF+%28%E6%9C%8D%E5%8A%A1%E5%99%A8%E5%A4%96%E7%BD%91ip%3A192.168.205.10+%E5%AE%A2%E6%88%B7%E7%AB%AFip%3A192.168.72.187%29%09%22%7B%22%22AbsolutePath%22%22%3A%22%22%2Flesson%2Fdetail%2F195555%22%22%2C%22%22ReferrerUrl%22%22%3A%22%22%22%22%2C%22%22QueryData%22%22%3A%22%22%22%22%2C%22%22FormData%22%22%3A%22%22%22%22%2C%22%22User%22%22%3A%7B%22%22Name%22%22%3A%22%22%22%22%2C%22%22IsAuthenticated%22%22%3Afalse%7D%2C%22%22ExData%22%22%3A%7B%22%22ExtendMessage%22%22%3Anull%2C%22%22ExceptionType%22%22%3A%22%22Nd.Tool.BusinessException%22%22%2C%22%22CauseMethod%22%22%3A%22%22System.Web.Mvc.ActionResult+Detail%28Int32%29%22%22%2C%22%22CauseSource%22%22%3A%22%22RenRenJiaoWeb%22%22%2C%22%22ErrorMessage%22%22%3A%22%22lesson%E5%8F%82%E6%95%B0%E9%94%99%E8%AF%AF%22%22%2C%22%22TraceStack%22%22%3A%22%22+++at+RenRenJiaoWeb.LessonController.Detail%28Int32+id%29%5Cr%5Cn+++at+lambda_method%28Closure+%2C+ControllerBase+%2C+Object%5B%5D+%29%5Cr%5Cn+++at+System.Web.Mvc.ReflectedActionDescriptor.Execute%28ControllerContext+controllerContext%2C+IDictionary%602+parameters%29%5Cr%5Cn+++at+System.Web.Mvc.ControllerActionInvoker.InvokeActionMethod%28ControllerContext+controllerContext%2C+ActionDescriptor+actionDescriptor%2C+IDictionary%602+parameters%29%5Cr%5Cn+++at+Nd.Web.Mvc.ApiControllerActionInvoker.InvokeActionMethod%28ControllerContext+controllerContext%2C+ActionDescriptor+actionDescriptor%2C+IDictionary%602+parameters%29%5Cr%5Cn+++at+System.Web.Mvc.ControllerActionInvoker.%5Cu003c%5Cu003ec__DisplayClass15.%5Cu003cInvokeActionMethodWithFilters%5Cu003eb__12%28%29%5Cr%5Cn+++at+System.Web.Mvc.ControllerActionInvoker.InvokeActionMethodFilter%28IActionFilter+filter%2C+ActionExecutingContext+preContext%2C+Func%601+continuation%29%5Cr%5Cn+++at+System.Web.Mvc.ControllerActionInvoker.InvokeActionMethodWithFilters%28ControllerContext+controllerContext%2C+IList%601+filters%2C+ActionDescriptor+actionDescriptor%2C+IDictionary%602+parameters%29%5Cr%5Cn+++at+System.Web.Mvc.ControllerActionInvoker.InvokeAction%28ControllerContext+controllerContext%2C+String+actionName%29%22%22%7D%7D%22%092014%2F10%2F17+17%3A42%3A26%0A");

        Tuple tuple = mock(Tuple.class);
        when(tuple.getValues())
                .thenReturn(temp);
        return tuple;
    }

    public static Tuple mockSysLogDetailTuple() {
        Tuple tuple = mock(Tuple.class);
        Object detailObj = getRandomSysLogDetail();
        Integer appId = ((SysLogDetail)detailObj).getAppId();
        when(tuple.getValueByField(ConstVariables.SYS_LOG_DETAIL_VALUE_FILED))
                .thenReturn(detailObj);
        when(tuple.getIntegerByField(ConstVariables.SYS_LOG_ORIGINAL_PARTITION_FIELD))
                .thenReturn(appId);
        return tuple;
    }

    private static Object getRandomSysLogDetail() {
        int rndNum = random.nextInt(5);

        SysLogDetail logDetail = new SysLogDetail(logLevel[rndNum], appId[rndNum],
                new Date(System.currentTimeMillis()));
        return logDetail;
    }

    private static Object getRandomOriginalSysLog() throws ParseException {
        int rndNum = random.nextInt(5);

        OriginalSysLog sysLog = new OriginalSysLog();
        sysLog.setLogLevel(4);
        sysLog.setAppId(10);
        sysLog.setCallStack("\"{\"\"AbsolutePath\"\":\"\"/lesson/detail/195555\"\",\"\"ReferrerUrl\"\":\"\"\"\",\"\"QueryData\"\":\"\"\"\",\"\"FormData\"\":\"\"\"\",\"\"User\"\":{\"\"Name\"\":\"\"\"\",\"\"IsAuthenticated\"\":false},\"\"ExData\"\":{\"\"ExtendMessage\"\":null,\"\"ExceptionType\"\":\"\"Nd.Tool.BusinessException\"\",\"\"CauseMethod\"\":\"\"System.Web.Mvc.ActionResult Detail(Int32)\"\",\"\"CauseSource\"\":\"\"RenRenJiaoWeb\"\",\"\"ErrorMessage\"\":\"\"lesson参数错误\"\",\"\"TraceStack\"\":\"\"   at RenRenJiaoWeb.LessonController.Detail(Int32 id)\\r\\n   at lambda_method(Closure , ControllerBase , Object[] )\\r\\n   at System.Web.Mvc.ReflectedActionDescriptor.Execute(ControllerContext controllerContext, IDictionary`2 parameters)\\r\\n   at System.Web.Mvc.ControllerActionInvoker.InvokeActionMethod(ControllerContext controllerContext, ActionDescriptor actionDescriptor, IDictionary`2 parameters)\\r\\n   at Nd.Web.Mvc.ApiControllerActionInvoker.InvokeActionMethod(ControllerContext controllerContext, ActionDescriptor actionDescriptor, IDictionary`2 parameters)\\r\\n   at System.Web.Mvc.ControllerActionInvoker.\\u003c\\u003ec__DisplayClass15.\\u003cInvokeActionMethodWithFilters\\u003eb__12()\\r\\n   at System.Web.Mvc.ControllerActionInvoker.InvokeActionMethodFilter(IActionFilter filter, ActionExecutingContext preContext, Func`1 continuation)\\r\\n   at System.Web.Mvc.ControllerActionInvoker.InvokeActionMethodWithFilters(ControllerContext controllerContext, IList`1 filters, ActionDescriptor actionDescriptor, IDictionary`2 parameters)\\r\\n   at System.Web.Mvc.ControllerActionInvoker.InvokeAction(ControllerContext controllerContext, String actionName)\"\"}}\"");
        sysLog.setLogMessage("user参数错误 (服务器外网ip:192.168.205.10 客户端ip:192.168.205.140)");
        sysLog.setLogDate(format.parse("2014/10/17 17:42:26\n"));
        sysLog.setLogDate(new Date(System.currentTimeMillis()));
        sysLog.generateFeatureCode();

        return sysLog;
    }
}
