package HAFPIS.service;

import HAFPIS.DAO.FaceTTDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.FaceRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.thid.THIDFace;
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
public class OneToF_Face implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(OneToF_Face.class);
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private String Face_tablename;
    private int[] tasktypes = new int[2];
    private int[] datatypes = new int[2];
    private SrchTaskDAO srchTaskDAO;

    @Override
    public void run() {
        srchTaskDAO = new SrchTaskDAO(tablename);
        if (type == CONSTANTS.FACE1TOF) {
            tasktypes[0] = 8;
            datatypes[0] = 6;
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
                    log.debug("sleeping");
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
        ExecutorService executorService = Executors.newFixedThreadPool(5);
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
                        Future<FaceRec> rec = executorService.submit(new Callable<FaceRec>() {
                            @Override
                            public FaceRec call() throws Exception {
                                FaceRec faceRec = new FaceRec();
                                faceRec.candid = new String(gallery.probeId).trim();
                                THIDFace.VerifyFeature verifyFeature = new THIDFace.VerifyFeature();
                                verifyFeature.feature1 = probe.facemnt[0];
                                verifyFeature.feature2 = gallery.facemnt[0];
                                THIDFace.VerifyFeature.Result result = HbieUtil.getInstance().hbie_FACE.process(verifyFeature);
                                faceRec.score = result.score;
                                return faceRec;
                            }
                        });
                        listF.add(rec);
                    }
                }
                for (int i = 0; i < listF.size(); i++) {
                    FaceRec faceRec = null;
                    try {
                        faceRec = listF.get(i).get();
                        faceRec.taskid = srchTaskBean.getTASKIDD();
                        faceRec.transno = srchTaskBean.getTRANSNO();
                        faceRec.probeid = srchTaskBean.getPROBEID();
                        faceRec.dbid = 0;
                        list.add(faceRec);
                    } catch (InterruptedException | ExecutionException e) {
                        log.info("Face_1ToF get record error, ", e);
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
