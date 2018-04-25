package HAFPIS.domain;

/**
 * 描述：
 * 作者：ZP
 * 创建时间:2018/3/12
 * 最后修改时间:2018/3/12
 */
public class HeartBeatBean {
    private String INSTANCENAME;
    private int RANKNO;
    private long UPDATETIME;

    public String getINSTANCENAME() {
        return INSTANCENAME;
    }

    public void setINSTANCENAME(String INSTANCENAME) {
        this.INSTANCENAME = INSTANCENAME;
    }

    public int getRANKNO() {
        return RANKNO;
    }

    public void setRANKNO(int RANKNO) {
        this.RANKNO = RANKNO;
    }

    public long getUPDATETIME() {
        return UPDATETIME;
    }

    public void setUPDATETIME(long UPDATETIME) {
        this.UPDATETIME = UPDATETIME;
    }
}
