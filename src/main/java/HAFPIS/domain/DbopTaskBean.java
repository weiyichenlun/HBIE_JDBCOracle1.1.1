package HAFPIS.domain;

/**
 * Created by ZP on 2017/5/15.
 */
public class DbopTaskBean {
    private String TaskIdd;
    private String TransNo;
    private String ProbeId;
    private int DataType;
    private int TaskType;
    private int Status;
    private int priority;
    private long UserId;
    private String IPAddr;
    private int DbId;
    private String BegTime;
    private String EndTime;
    private String ExptMsg;

    public String getTaskIdd() {
        return TaskIdd;
    }

    public void setTaskIdd(String taskIdd) {
        TaskIdd = taskIdd;
    }

    public String getTransNo() {
        return TransNo;
    }

    public void setTransNo(String transNo) {
        TransNo = transNo;
    }

    public String getProbeId() {
        return ProbeId;
    }

    public void setProbeId(String probeId) {
        ProbeId = probeId;
    }

    public int getDataType() {
        return DataType;
    }

    public void setDataType(int dataType) {
        DataType = dataType;
    }

    public int getTaskType() {
        return TaskType;
    }

    public void setTaskType(int taskType) {
        TaskType = taskType;
    }

    public int getStatus() {
        return Status;
    }

    public void setStatus(int status) {
        Status = status;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getUserId() {
        return UserId;
    }

    public void setUserId(long userId) {
        UserId = userId;
    }

    public String getIPAddr() {
        return IPAddr;
    }

    public void setIPAddr(String IPAddr) {
        this.IPAddr = IPAddr;
    }

    public int getDbId() {
        return DbId;
    }

    public void setDbId(int dbId) {
        DbId = dbId;
    }

    public String getBegTime() {
        return BegTime;
    }

    public void setBegTime(String begTime) {
        BegTime = begTime;
    }

    public String getEndTime() {
        return EndTime;
    }

    public void setEndTime(String endTime) {
        EndTime = endTime;
    }

    public String getExptMsg() {
        return ExptMsg;
    }

    public void setExptMsg(String exptMsg) {
        ExptMsg = exptMsg;
    }
}
