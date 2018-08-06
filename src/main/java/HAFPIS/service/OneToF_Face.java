package HAFPIS.service;

import HAFPIS.DAO.FaceTTDAO;
import HAFPIS.DAO.HeartBeatDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.FaceRec;
import HAFPIS.domain.HeartBeatBean;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.thid.THIDFace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 描述：
 * 作者：ZP
 * 创建时间:2017/5/19
 * 最后修改时间:2017/5/19
 */
public class OneToF_Face extends Recog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(OneToF_Face.class);

    private String Face_tablename;
    private ExecutorService executorService = Executors.newFixedThreadPool(5);


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
        srchTaskDAO = new SrchTaskDAO(tablename);
        if (type == CONSTANTS.FACE1TOF) {
            tasktypes[0] = 8;
            datatypes[0] = 6;
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
            System.out.println("Face1ToF executorservice is shutting down");
        }));
        while (true) {
            List<SrchTaskBean> list = new ArrayList<>();
            try {
                list = srchTaskDAO.getList(status, datatypes, tasktypes, queryNum);
            } catch (SQLException e) {
                log.error("1tof face database error. ", e);
                CommonUtil.sleep("10");
                continue;
            }
            CommonUtil.checkList(list, interval);
            SrchTaskBean srchTaskBean = null;
            for (int i = 0; i < list.size(); i++) {
                srchTaskBean = list.get(i);
                //srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
                Blob srchdata = srchTaskBean.getSRCHDATA();
//                byte[] srchdata = srchTaskBean.getSRCHDATA();
                int dataType = srchTaskBean.getDATATYPE();
                if (srchdata != null) {
                    List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                    if (srchDataRecList == null || srchDataRecList.size() <= 0) {
                        log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
                        srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "can not get srchdata");
                    } else {
                        Face(srchDataRecList, srchTaskBean);
                    }
                } else {
                    log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                }
            }
        }

    }

    private void Face(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        FaceTTDAO facedao = new FaceTTDAO(Face_tablename);
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
            log.error("there is only one SrchDataRec in srchDataRecList, FPLL_1ToF will stop");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "only one srchdata record");
        } else {
            List<FaceRec> list = new ArrayList<>();
            SrchDataRec probe = srchDataRecList.get(0);
            if (probe.latfpmnt == null) {
                exptMsg.append("probe latfpmnt is null.");
                log.error("the probe latfpmnt is null. probeid={}", new String(probe.probeId));
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "probe latfpmnt is null");
            } else {
                Map<String, Future<Float>> map = new HashMap<>();
                List<Future<FaceRec>> listF = new ArrayList<>();
                for (int i = 1; i < srchDataRecList.size(); i++) {
                    SrchDataRec gallery = srchDataRecList.get(i);
                    if (gallery.facemnt == null) {
                        log.warn("gallery facemnt is null! the position is {} and probeid is {}", i + 1, new String(gallery.probeId));
                    } else {
                        Future<FaceRec> rec = executorService.submit(() -> {
                            FaceRec faceRec = new FaceRec();
                            faceRec.candid = new String(gallery.probeId).trim();
                            THIDFace.VerifyFeature verifyFeature = new THIDFace.VerifyFeature();
                            verifyFeature.feature1 = probe.facemnt[0];
                            verifyFeature.feature2 = gallery.facemnt[0];
                            THIDFace.VerifyFeature.Result result = HbieUtil.getInstance().hbie_FACE.process(verifyFeature);
                            faceRec.score = result.score;
                            return faceRec;
                        });
                        listF.add(rec);
                    }
                }
                for (Future<FaceRec> aListF : listF) {
                    FaceRec faceRec = null;
                    try {
                        faceRec = aListF.get();
                        faceRec.taskid = srchTaskBean.getTASKIDD();
                        faceRec.transno = srchTaskBean.getTRANSNO();
                        faceRec.probeid = srchTaskBean.getPROBEID();
                        faceRec.dbid = 0;
                        list.add(faceRec);
                    } catch (InterruptedException | ExecutionException e) {
                        log.info("Face_1ToF get record error, ", e);
                        continue;
                    }
                }
            }
            list = CommonUtil.mergeResult(list);
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("Face_1ToF search: No results. ProbeId={}, ExceptionMsg={}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("Face_1ToF search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                boolean isSuc = facedao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("Face_1ToF search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(Face_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("Face_1ToF search results insert into {} error. ProbeId={}", Face_tablename, srchTaskBean.getPROBEID());
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

    public String getFace_tablename() {
        return Face_tablename;
    }

    public void setFace_tablename(String face_tablename) {
        Face_tablename = face_tablename;
    }
}
