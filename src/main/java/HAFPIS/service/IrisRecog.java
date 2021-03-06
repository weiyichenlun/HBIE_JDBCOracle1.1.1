package HAFPIS.service;

import HAFPIS.DAO.HeartBeatDAO;
import HAFPIS.DAO.IrisTTDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.HeartBeatBean;
import HAFPIS.domain.IrisRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.MatcherException;
import com.hisign.bie.SearchResults;
import com.hisign.bie.iris.HSIris;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 虹膜比对
 * Created by ZP on 2017/5/17.
 */
public class IrisRecog extends Recog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(IrisRecog.class);
    private static ArrayBlockingQueue<SrchTaskBean> irisArrayQueue;
    private float  IrisTT_threshold;
    private String IrisTT_tablename;

    @Override
    public void run() {
        String heartBeatTable = ConfigUtil.getConfig("heart_beat_table");
        String instanceName = ConfigUtil.getConfig("instance_name");
        if (heartBeatTable == null || instanceName == null) {
            log.warn("No heartbeat config found. ");
        } else {
            CommonUtil.sleep("" + 3);
            heartBeatDAO = new HeartBeatDAO(heartBeatTable);
            while (true) {
                CommonUtil.sleep("" + CONSTANTS.SLEEP_TIME);
                HeartBeatBean bean = heartBeatDAO.queryLatest();
                if (bean == null) {
                    try {
                        Thread.sleep(3 * 1000);
                        continue;
                    } catch (InterruptedException e) {
                    }
                }
                if (bean.getINSTANCENAME().equals(instanceName) && bean.getUPDATETIME() > 0) {
                    log.info("Current active instance is {}, this instance is {}", bean.getINSTANCENAME(), instanceName);
                    break;
                } else if (!bean.getINSTANCENAME().equals(instanceName)) {
                    log.debug("Current active instance: {}, but this instance is {}", bean.getINSTANCENAME(), instanceName);
                    try {
                        Thread.sleep(3 * 1000);
                    } catch (InterruptedException e) {
                        log.error("Error. ", e);
                    }
                }
            }
        }
        log.info("start IrisRecog");
        datatypes = new int[]{7};
        if (type == CONSTANTS.IRIS) {
            tasktypes[0] = 1;
        }
        log.info("Starting...Update status first...");
        srchTaskDAO = new SrchTaskDAO(tablename);
        while (true) {
            try {
                srchTaskDAO.updateStatus(datatypes, tasktypes);
                break;
            } catch (Exception e) {
                log.error("database error. ", e);
                CommonUtil.sleep("10");
                continue;
            }
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("----------------");
            boundedExecutor.close();
            while (true) {
                try {
                    srchTaskDAO.updateStatus(new int[]{7}, tasktypes);
                    break;
                } catch (Exception e) {
                    log.error("database error. ", e);
                    CommonUtil.sleep("10");
                    continue;
                }
            }
            System.out.println("Iris executorservice is shutting down");
        }));

        String irisMatcherShardsStr = ConfigUtil.getConfig("iris_matcher_shards");
        Integer irisMatcherShards = 1;
        try {
            irisMatcherShards = Integer.parseInt(irisMatcherShardsStr);
        } catch (NumberFormatException e) {
            log.error("iris_matcher_shards {} is not a number. ", irisMatcherShardsStr, e);
        }

        irisArrayQueue = new ArrayBlockingQueue<>(irisMatcherShards * 2);
        if (tasktypes[0] == 1) {
            Integer finalIrisMatcherShards = irisMatcherShards;
            new Thread(() -> {
            while (true) {
                List<SrchTaskBean> list = null;
                try {
                    list = srchTaskDAO.getSrchTaskBean(3, 7, 1, finalIrisMatcherShards);
                } catch (Exception e) {
                    log.error("facett database error.", e);
                    CommonUtil.sleep("10");
                    continue;
                }
                if (list == null || list.size() == 0) {
                        CommonUtil.sleep(interval);
                    } else {
                        for (SrchTaskBean srchTaskBean : list) {
                            try {
                                irisArrayQueue.put(srchTaskBean);
                                //srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
                            } catch (InterruptedException e) {
                                log.error("Putting into iris queue error. ", e);
                                CommonUtil.sleep("10");
                            }
                        }
                    }
                }
            }, "facett_srchtaskbean_thread").start();
            for (int i = 0; i < irisMatcherShards; i++) {
                new Thread(this::IrisTT, "IrisTT_Thread_" + (i + 1)).start();
            }
        }
    }

    private void IrisTT() {
        while (true) {
            SrchTaskBean srchTaskBean = null;
            try {
                srchTaskBean = irisArrayQueue.take();
            } catch (InterruptedException e) {
                log.error("take srchtaskbean from iris Array queue error.", e);
                CommonUtil.sleep("10");
                continue;
            }
            Blob srchdata = srchTaskBean.getSRCHDATA();
//            byte[] srchdata = srchTaskBean.getSRCHDATA();
            int dataType = srchTaskBean.getDATATYPE();
            if (srchdata != null) {
                List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                if (srchDataRecList == null || srchDataRecList.size() <= 0) {
                    log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "can not get srchdata");
                } else {
                    IrisTT(srchDataRecList, srchTaskBean);
                }
                } else {
                    log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
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

            String dbFilter = CommonUtil.getDBsFilter(srchTaskBean.getSRCHDBSMASK());
            String demoFilter = CommonUtil.getFilter(srchTaskBean.getDEMOFILTER());
            log.info(srchTaskBean.getSRCHDBSMASK());
            probe.filter = CommonUtil.mergeFilter(dbFilter, demoFilter);
            log.info("The total filter is :\n{}", probe.filter);
            probe.scoreThreshold = IrisTT_threshold;
            int numOfCand = srchTaskBean.getNUMOFCAND();
            if (numOfCand > 0) {
                probe.maxCands = (int) (numOfCand * 1.5);
            }else{
                probe.maxCands = numOfCand = CONSTANTS.MAXCANDS;
            }

            SearchResults<HSIris.IrisSearchParam.Result> results = HbieUtil.getInstance().hbie_IRIS.search(probe);
            for(HSIris.IrisSearchParam.Result cand:results.candidates){
                IrisRec irisRec = new IrisRec();
                irisRec.taskid = srchTaskBean.getTASKIDD();
                irisRec.transno = srchTaskBean.getTRANSNO();
                irisRec.probeid = srchTaskBean.getPROBEID();
                irisRec.candid = cand.record.id;
                irisRec.dbid = (int) cand.record.info.get("dbId");
                irisRec.score = cand.score;
                irisRec.iiscores = cand.scores;
                list.add(irisRec);
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
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(IrisTT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("IrisTT search results insert into {} error. ProbeId={}", IrisTT_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
            CommonUtil.sleep("10");
        } catch (MatcherException var7) {
            log.error("IrisTT Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
            CommonUtil.sleep("10");
        } catch (Exception e) {
            String temp = exptMsg.toString() + e.toString();
            if (e instanceof IllegalArgumentException) {
                log.error("IrisTT illegal parameters error. ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, temp.length() > 128? temp.substring(0, 128):temp);
            } else {
                log.error("IrisTT exception ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, temp.length() > 128? temp.substring(0, 128):temp);
                CommonUtil.sleep("10");
            }
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
