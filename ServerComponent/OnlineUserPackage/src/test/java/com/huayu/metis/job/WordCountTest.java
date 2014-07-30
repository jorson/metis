package com.huayu.metis.job;

import org.junit.Test;

import java.util.Calendar;

/**
 * Created by Administrator on 14-7-29.
 */
public class WordCountTest {

    @Test
    public void calenderTest() {
        Calendar calendar = Calendar.getInstance();
        calendar.setFirstDayOfWeek(Calendar.MONDAY);

        System.out.println(calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        System.out.println(calendar.getActualMinimum(Calendar.DAY_OF_MONTH));

        System.out.println(calendar.getActualMaximum(Calendar.DAY_OF_WEEK));
        System.out.println(calendar.getActualMinimum(Calendar.DAY_OF_WEEK));

        Long[] results = new Long[2];
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        System.out.println(calendar.get(Calendar.DAY_OF_MONTH));
        results[0] = calendar.getTimeInMillis();

        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
        System.out.println(calendar.get(Calendar.DAY_OF_MONTH));
        results[1] = calendar.getTimeInMillis();

        System.out.println(results[0]);
        System.out.println(results[1]);
    }
}
