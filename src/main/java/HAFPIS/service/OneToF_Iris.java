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
import com.hisign.bie.iris.HSIris;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by ZP on 2017/5/19.
 */
public class OneToF_Iris extends Recog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(OneToF_Iris.class);

    private String Iris_tablename;

    ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Override
    public void run() {
        String heartBeatTable = ConfigUtil.getConfig("heart_beat_table");
        String instanceName = ConfigUtil.getConfig("instance_name");
        if (heartBeatTable == null || instanceName == null) {
            log.warn("No heartbeat config found. ");
        } else {
            CommonUtil.sleep("" + CONSTANTS.SLEEP_TIME);
            heartBeatDAO = new HeartBeatDAO(heartBeatTable);
            while (true) {
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
        log.info("start OneToF_Iris");
        srchTaskDAO = new SrchTaskDAO(tablename);
        if (type == CONSTANTS.IRIS1TOF) {
            tasktypes[0] = 8;
            datatypes[0] = 7;
        } else{
            log.warn("the type is wrong. type={}", type);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("----------------");
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            executorService.shutdown();
            while (true) {
                try {
                    srchTaskDAO.updateStatus(datatypes, tasktypes);
                    break;
                } catch (SQLException e) {
                    log.error("database error. ", e);
                    CommonUtil.sleep("10");
                    continue;
                }
            }
            System.out.println("Iris1ToF executorservice is shutting down");
        }));
        while (true) {
            List<SrchTaskBean> list = new ArrayList<>();
            try {
                list = srchTaskDAO.getList(status, datatypes, tasktypes, queryNum);
            } catch (SQLException e) {
                log.error("1tof iris database error. ", e);
                CommonUtil.sleep("10");
                continue;
            }
            CommonUtil.checkList(list, interval);
            SrchTaskBean srchTaskBean = null;
            for (int i = 0; i < list.size(); i++) {
                srchTaskBean = list.get(i);
//                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
                Blob srchdata = srchTaskBean.getSRCHDATA();
//                byte[] srchdata = srchTaskBean.getSRCHDATA();
                int dataType = srchTaskBean.getDATATYPE();
                if (srchdata != null) {
                    List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                    if (srchDataRecList == null || srchDataRecList.size() <= 0) {
                        log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
                        srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "can not get srchdata");
                    } else {
                        Iris(srchDataRecList, srchTaskBean);
                    }
                } else {
                    log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                }
            }
        }
    }

    private void Iris(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        IrisTTDAO irisdao = new IrisTTDAO(Iris_tablename);
        String tempMsg = srchTaskBean.getEXPTMSG();
        StringBuilder exptMsg;
        if (tempMsg == null) {
            exptMsg = new StringBuilder();
        } else {
            exptMsg = new StringBuilder(tempMsg);
        }
        if (srchDataRecList.size() <= 1) {
            srchTaskBean.setSTATUS(-1);
            srchTaskBean.setEXPTMSG("there is only one SrchDataRec");
            log.error("there is only one SrchDataRec in srchDataRecList, Iris_1ToF will stop");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "only one srchdata record");
        } else {
            List<IrisRec> list = new ArrayList<>();
            SrchDataRec probe = srchDataRecList.get(0);
            if (probe.irismntnum == 0) {
                exptMsg.append("probe irismnt are both null.");
                log.error("the probe iris are both null. probeid={}", new String(probe.probeId));
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "probe irismnt are both null");
            } else {
                Map<String, Future<Float>> map = new HashMap<>();
                List<Future<IrisRec>> listF = new ArrayList<>();
                for (int i = 1; i < srchDataRecList.size(); i++) {
                    SrchDataRec gallery = srchDataRecList.get(i);
                    if (gallery.irismntnum == 0) {
                        log.warn("gallery irismnt are both null! the position is {} and probeid is {}", i + 1, new String(gallery.probeId));
                    } else {
                        Future<IrisRec> rec = executorService.submit(new Callable<IrisRec>() {
                            @Override
                            public IrisRec call() throws Exception {
                                IrisRec irisRec = new IrisRec();
                                irisRec.candid = new String(gallery.probeId).trim();
                                HSIris.VerifyFeature verifyFeature = new HSIris.VerifyFeature();
                                for (int j = 0; j < 2; j++) {
                                    byte[] fea1 = probe.irismnt[j];
                                    byte[] fea2 = gallery.irismnt[j];
                                    if (fea1 != null && fea2 != null) {
                                        verifyFeature.feature1 = fea1;
                                        verifyFeature.feature2 = fea2;
                                        HSIris.VerifyFeature.Result result = HbieUtil.getInstance().hbie_IRIS.process(verifyFeature);
                                        irisRec.iiscores[j] = result.score;
                                    } else {
                                        irisRec.iiscores[j] = 0F;
                                    }
                                }
                                irisRec.score = Math.max(irisRec.iiscores[0], irisRec.iiscores[1]);
                                return irisRec;
                            }
                        });
                        listF.add(rec);
                    }
                }
                for (int i = 0; i < listF.size(); i++) {
                    IrisRec irisRec = null;
                    try {
                        irisRec = listF.get(i).get();
                        irisRec.taskid = srchTaskBean.getTASKIDD();
                        irisRec.transno = srchTaskBean.getTRANSNO();
                        irisRec.probeid = srchTaskBean.getPROBEID();
                        irisRec.dbid = 0;
                        list.add(irisRec);
                    } catch (InterruptedException | ExecutionException e) {
                        log.info("Iris_1ToF get record error, ", e);
                    }
                }
            }
            list = CommonUtil.mergeResult(list);
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("Iris_1ToF search: No results. ProbeId={}, ExceptionMsg={}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("Iris_1ToF search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                boolean isSuc = irisdao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("Iris_1ToF search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(Iris_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("Iris_1ToF search results insert into {} error. ProbeId={}", Iris_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
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

    public String getIris_tablename() {
        return Iris_tablename;
    }

    public void setIris_tablename(String iris_tablename) {
        Iris_tablename = iris_tablename;
    }
}
