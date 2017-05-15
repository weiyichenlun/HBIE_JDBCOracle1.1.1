package HAFPIS.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 指纹识别 TT和LT
 * Created by ZP on 2017/5/15.
 */
public class FpRecog implements Runnable {
    private final Logger log = LoggerFactory.getLogger(FpRecog.class);
    private int type;
    private int interval;
    private int queryNum;
    private int status;


    @Override
    public void run() {

        String query_sel = "SELECT * FROM HAFPIS_SRCH_TASK WHERE STATUS = 3 AND TASKTYPE IN (1,3)AND ROWNUM<=20 ORDER BY PRIORITY DESC, BEGTIME ASC";
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public int getQueryNum() {
        return queryNum;
    }

    public void setQueryNum(int queryNum) {
        this.queryNum = queryNum;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
