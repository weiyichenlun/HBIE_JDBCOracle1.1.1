package HAFPIS.service;

import HAFPIS.DAO.IrisTTDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.IrisRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.iris.HSIris;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by ZP on 2017/5/19.
 */
public class OneToF_Iris implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(OneToF_Iris.class);
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private String Iris_tablename;
    private int[] tasktypes = new int[2];
    private int[] datatypes = new int[2];
    private SrchTaskDAO srchTaskDAO;

    @Override
    public void run() {
        srchTaskDAO = new SrchTaskDAO(tablename);
        if (type == CONSTANTS.IRIS1TOF) {
            tasktypes[0] = 8;
            datatypes[0] = 7;
        } else{
            log.warn("the type is wrong. type={}", type);
        }
        while (true) {
            List<SrchTaskBean> list = new ArrayList<>();
            list = srchTaskDAO.getList(status, datatypes, tasktypes, queryNum);
            if ((list.size() == 0)) {
                int timeSleep = 1;
                try {
                    timeSleep = Integer.parseInt(interval);
                } catch (NumberFormatException e) {
                    log.error("interval {} format error. Use default interval(1)", interval);
                }
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
                if (srchdata != null) {
                    List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                    if (srchDataRecList == null || srchDataRecList.size() <= 0) {
                        log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
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
        ExecutorService executorService = Executors.newFixedThreadPool(5);
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
