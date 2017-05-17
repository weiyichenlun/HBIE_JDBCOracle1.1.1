package HAFPIS.service;

import HAFPIS.DAO.IrisTTDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.IrisRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.MatcherException;
import com.hisign.bie.SearchResults;
import com.hisign.bie.iris.HSIris;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ZP on 2017/5/17.
 */
public class IrisRecog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(IrisRecog.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private float  IrisTT_threshold;
    private String IrisTT_tablename;
    int[] tasktypes = new int[2];
    private SrchTaskDAO srchTaskDAO;

    @Override
    public void run() {
        if (type == CONSTANTS.IRIS) {
            tasktypes[0] = 1;
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
                    Thread.sleep(timeSleep * 1000);
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
                                    long start = System.currentTimeMillis();
                                    IrisTT(srchDataRecList, srchTaskBean);
                                    log.info("IrisTT total cost : {} ms", (System.currentTimeMillis() - start));
                                    break;
                            }
                        }
                    } else {
                        log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                        srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                    }
                } catch (Exception e) {

                }
            }
        }
    }

    private void IrisTT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSIris.IrisSearchParam probe = new HSIris.IrisSearchParam();
        IrisTTDAO irisTTDAO = new IrisTTDAO(IrisTT_tablename);
        String tempMsg = srchTaskBean.getEXPTMSG();
        StringBuilder exptMsg;
        if (tempMsg == null) {
            exptMsg = new StringBuilder();
        }else {
            exptMsg = new StringBuilder(tempMsg);
        }

        SrchDataRec srchDataRec = srchDataRecList.get(0);
        byte[][] features = srchDataRec.irismnt;
        if (srchDataRec.irismntnum == 0) {
            exptMsg.append("IrisTT features are null");
            log.warn("IrisTT: IrisMnt features are null. PreobeId={}", srchTaskBean.getPROBEID());
        }
        try{
            List<IrisRec> list = new ArrayList<>();
            probe.features = features;
            probe.id = srchTaskBean.getPROBEID();
            int numOfCand = srchTaskBean.getNUMOFCAND();
            if (numOfCand > 0) {
                probe.maxCands = (int) (numOfCand * 1.5);
            }else{
                probe.maxCands = numOfCand = CONSTANTS.MAXCANDS;
            }

            SearchResults<HSIris.IrisSearchParam.Result> results = HbieUtil.hbie_IRIS.search(probe);
            for(HSIris.IrisSearchParam.Result cand:results.candidates){
                IrisRec irisRec = new IrisRec();
                irisRec.taskid = srchTaskBean.getTASKIDD();
                irisRec.transno = srchTaskBean.getTRANSNO();
                irisRec.probeid = srchTaskBean.getPROBEID();
                irisRec.dbid = 1;
                irisRec.candid = cand.record.id;
                irisRec.score = cand.score;
                irisRec.iiscores = cand.scores;
                if (irisRec.score > IrisTT_threshold) {
                    list.add(irisRec);
                }
            }
            if((list.size() == 0)){
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("IrisTT search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("IrisTT search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            }else{
                list = CommonUtil.mergeResult(list);
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                }
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                log.info("begin to write results into {}", IrisTT_tablename);
                boolean isSuc = irisTTDAO.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("IrisTT search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, null);
                } else {
                    exptMsg.append(IrisTT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("FaceTT search results insert into {} error. ProbeId={}", IrisTT_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString().substring(1, 128));
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString().substring(0,128));
        } catch (MatcherException var7) {
            log.error("IrisTT Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString().substring(0, 128));
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

    public float getIrisTT_threshold() {
        return IrisTT_threshold;
    }

    public void setIrisTT_threshold(float irisTT_threshold) {
        IrisTT_threshold = irisTT_threshold;
    }

    public String getIrisTT_tablename() {
        return IrisTT_tablename;
    }

    public void setIrisTT_tablename(String irisTT_tablename) {
        IrisTT_tablename = irisTT_tablename;
    }
}
