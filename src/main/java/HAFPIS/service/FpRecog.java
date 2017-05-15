package HAFPIS.service;

import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.MatcherException;
import com.hisign.bie.SearchResults;
import com.hisign.bie.hsfp.HSFPTenFp;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 指纹识别 TT和LT
 * Created by ZP on 2017/5/15.
 */
public class FpRecog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(FpRecog.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private String FPTT_tablename;
    private String FPLT_tablename;
    int[] tasktypes = new int[2];
    private SrchTaskDAO srchTaskDAO;

    @Override
    public void run() {
        if (type == CONSTANTS.FPTT) {
            tasktypes[0] = 1;
        } else if (type == CONSTANTS.FPLT) {
            tasktypes[1] = 3;
        } else if (type == CONSTANTS.FPTTLT) {
            tasktypes[0] = 1;
            tasktypes[1] = 3;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tablename);
        sb.append(" where status=").append(Integer.parseInt(status));
        sb.append(" and tasktype in (");
        for (int tasktype : tasktypes) {
            if (tasktype != 0) {
                sb.append(tasktype).append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1).append(")");
        sb.append(" and rownum<=").append(Integer.parseInt(queryNum));
        sb.append(" order by priority desc, begtime asc");
        srchTaskDAO = new SrchTaskDAO(tablename);
        while (true) {
            List<SrchTaskBean> list = new ArrayList<>();
            try {
                list = qr.query(sb.toString(), new BeanListHandler<SrchTaskBean>(SrchTaskBean.class));
                System.out.println(sb.toString());
            } catch (SQLException e) {
                log.error("SQLException: {}, query_sql:{}", e, sb.toString());
            }
            if ((list.size() == 0)) {
                int timeSleep = Integer.parseInt(interval);
                try {
                    Thread.sleep(timeSleep * 10000);
                    log.info("sleeping");
                } catch (InterruptedException e) {
                    log.warn("Waiting Thread was interrupted: {}", e);
                }
            }
            SrchTaskBean srchTaskBean = null;
            for (int i = 0; i < list.size(); i++) {
                srchTaskBean = list.get(i);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
                Blob srchdata = srchTaskBean.getSRCHDATA();
                int dataType = srchTaskBean.getDATATYPE();
                try {
                    if (srchdata != null) {
                        List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                        if (srchDataRecList.size() <= 0) {
                            log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
                        } else {
                            int tasktype = srchTaskBean.getTASKTYPE();
                            switch (tasktype) {
                                case 1:
                                    FPTT(srchDataRecList, srchTaskBean);
                                    break;
                                case 3:
                                    FPLT(srchDataRecList, srchTaskBean);
                                    break;
                            }
                        }
                    } else {
                        log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                        //TODO 更新SrchTask表
                        srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                    }
                } catch (Exception e) {

                }
            }
        }
    }

    private void FPLT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {

    }

    private void FPTT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPTenFp.TenFpSearchParam probe = new HSFPTenFp.TenFpSearchParam();
        String tempMsg = srchTaskBean.getEXPTMSG();
        StringBuilder exptMsg;
        float threshold = 0F;
        float[][] scores = new float[2][10];
        if(tempMsg==null)
            exptMsg = new StringBuilder();
        else
            exptMsg = new StringBuilder(tempMsg);
        SrchDataRec srchDataRec = srchDataRecList.get(0);
        byte[][] features_roll = srchDataRec.rpmnt;
        byte[][] features_flat = srchDataRec.fpmnt;
        //判断特征是否为空
        if (srchDataRec.rpmntnum == 0 && srchDataRec.fpmntnum == 0) {
            exptMsg.append(" RollMnt and FlatMnt features are both null ");
            log.warn("FPTT: RollMnt and FlatMnt features are both null. ProbeId=", srchTaskBean.getPROBEID());
        }
        try{
            List<FPRec> list = new ArrayList<>();
            probe.features = features_roll;
            probe.id = srchTaskBean.getPROBEID();
            int numOfCand = srchTaskBean.getNUMOFCAND();
            if(numOfCand >0)
                probe.maxCands = (int)(numOfCand*1.5);
            else
                probe.maxCands = MAXCANDS;
            probe.filter = "flag=={0}";
            SearchResults<HSFPTenFp.TenFpSearchParam.Result> results = null;
            results = HBIEinit.hbie_FP.search(probe);
            for(HSFPTenFp.TenFpSearchParam.Result cand:results.candidates){
                FPRec fpRec = new FPRec();
                fpRec.candId = cand.record.id;
                scores[0] = normalScore(cand.fpscores[5]);
                fpRec.score = cand.score;
                fpRec.info = cand.record.info;
                if(fpRec.score>=threshold) {
                    list.add(fpRec);
                }
            }
            probe.features = features_flat;
            probe.id = srchTaskBean.getPROBEID();
            if(numOfCand >0)
                probe.maxCands = (int)(numOfCand*1.5);
            else
                probe.maxCands = MAXCANDS;
            probe.filter = "flag=={1}";
            results = HBIEinit.hbie_FP.search(probe);
            for(HSFPTenFp.TenFpSearchParam.Result cand:results.candidates){
                FPRec fpRec = new FPRec();
                fpRec.candId = cand.record.id;
                scores[1] = normalScore(cand.fpscores[5]);
                fpRec.score = cand.score;
                fpRec.info = cand.record.info;
                if(fpRec.score>=threshold) {
                    list.add(fpRec);
                }
            }
            list = Utils.mergeResult(list);
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("TT search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("TT search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                }
            } else {
                if (list.size() > numOfCand) {
                    list = Utils.getList(list, numOfCand);
                }
                String taskIdd = srchTaskBean.getTASKIDD();
                String transNo = srchTaskBean.getTRANSNO();
                String probeId = srchTaskBean.getPROBEID();
                boolean isSuc = updateTable_TT(taskIdd, transNo, probeId, scores, list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("TT search finished. ProbeId={}", probeId);
                } else {
                    exptMsg.append(tableName).append(" Insert error");
                    log.error("TT search results insert into {} error. ProbeId={}", tableName, probeId);
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            throw new RemoteException(exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("FPTT Matcher error: ", var7);
            log.info("try to restart Matcher...");
            startTenFpMatcher();
            updateSrchTask(3, srchTaskBean.getPROBEID());
//            throw new MatcherException(exptMsg.toString());
        }
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getInterval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }

    public String getQueryNum() {
        return queryNum;
    }

    public void setQueryNum(String queryNum) {
        this.queryNum = queryNum;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public String getFPTT_tablename() {
        return FPTT_tablename;
    }

    public void setFPTT_tablename(String FPTT_tablename) {
        this.FPTT_tablename = FPTT_tablename;
    }

    public String getFPLT_tablename() {
        return FPLT_tablename;
    }

    public void setFPLT_tablename(String FPLT_tablename) {
        this.FPLT_tablename = FPLT_tablename;
    }

    public static void main(String[] args) {
        int num = 0;
        String interval = "1";
        String querynum = "10";
        String status = "3";
        String type = null;
        String tablename = null;
        String FPTT_tablename = null;
        String FPLT_tablename = null;
        Properties prop = new Properties();

        if (args == null) {
            log.info("请输入一个配置文件名称(例如HSFP.properties):  ");
            System.exit(-1);
        } else {
            String name = args[0];
            String temp = null;
            if (name.startsWith("-")) {
                if (name.startsWith("-cfg-file=")) {
                    temp = name.substring(name.indexOf(61) + 1);
                    prop = ConfigUtil.getProp(temp);
                } else {
                    int t = name.indexOf(61);
                    if (t == -1) {
                        temp = name;
                        prop = ConfigUtil.getProp(temp);
                    } else {
                        temp = name.substring(t + 1);
                        prop = ConfigUtil.getProp(temp);
                    }
                }

                type = (String) prop.get("type");
                interval = (String) prop.get("interval");
                querynum = (String) prop.get("querynum");
                status = (String) prop.get("status");
                tablename = (String) prop.get("tablename");
                FPTT_tablename = (String) prop.get("FPTT_tablename");
                FPLT_tablename = (String) prop.get("FPLT_tablename");
            }
            if (type == null) {
                log.error("没有指定type类型，无法启动程序");
                System.exit(-1);
            } else {
                String[] types = type.split("[,;\\s]+");
                if (types.length == 2) {
                    if ((types[0].equals("TT") && types[1].equals("LT")) || (types[0].equals("LT") && types[1].equals("TT"))) {
                        num = CONSTANTS.FPTTLT;
                    } else {
                        log.warn("配置文件指定类型错误. ", type);
                        num = -1;
                    }
                } else if (types.length == 1) {
                    switch (types[0]) {
                        case "TT":
                            num = CONSTANTS.FPTT;
                            break;
                        case "LT":
                            num = CONSTANTS.FPLT;
                            break;
                        default:
                            log.warn("type error.");
                            break;
                    }
                } else {
                    log.error("配置文件指定类型错误. ", type);
                    num = -1;
                }
            }
            if (num != -1) {
                FpRecog fpRecog = new FpRecog();
                fpRecog.setType(num);
                fpRecog.setInterval(interval);
                fpRecog.setStatus(status);
                fpRecog.setQueryNum(querynum);
                fpRecog.setTablename(tablename);
                fpRecog.setFPTT_tablename(FPTT_tablename);
                fpRecog.setFPLT_tablename(FPLT_tablename);
                Thread fpThread = new Thread(fpRecog);
                fpThread.start();
            }
        }
    }
}
