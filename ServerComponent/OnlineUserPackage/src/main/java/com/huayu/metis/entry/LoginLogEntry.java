package com.huayu.metis.entry;

import java.text.ParseException;

/**
 * Created by Administrator on 14-7-11.
 */
public class LoginLogEntry extends BaseLogEntry {

    protected int identityType;
    protected int solutionId;
    protected long loginTime;

    public LoginLogEntry() {
        super();
        identityType = 1;
        solutionId = -1;
        loginTime = System.currentTimeMillis();
    }

    @Override
    public void parse(String valueString) throws ParseException {

    }

    public int getIdentityType() {
        return identityType;
    }

    public int getSolutionId() {
        return solutionId;
    }

    public long getLoginTime() {
        return loginTime;
    }
}
