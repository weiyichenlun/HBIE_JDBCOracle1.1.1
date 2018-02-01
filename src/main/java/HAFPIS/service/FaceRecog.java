package HAFPIS.service;

import HAFPIS.DAO.FaceTTDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.FaceRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.MatcherException;
import com.hisign.bie.SearchResults;
import com.hisign.bie.thid.THIDFace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 人脸比对 TT
 * Created by ZP on 2017/5/17.
 */
public class FaceRecog extends Recog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(FaceRecog.class);
    private static ArrayBlockingQueue<SrchTaskBean>  faceArrayQueue;
    private float  FaceTT_threshold;
    private String FaceTT_tablename;


    @Override
    public void run() {
        datatypes = new int[]{6};
        if (type == CONSTANTS.FACE) {
            tasktypes[0] = 1;
        }
        log.info("Starting...Update status first...");
        srchTaskDAO = new SrchTaskDAO(tablename);
        srchTaskDAO.updateStatus(datatypes, tasktypes);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("----------------");
            boundedExecutor.close();
            srchTaskDAO.updateStatus(new int[]{6}, tasktypes);
            System.out.println("Face executorservice is shutting down");
        }));

        String faceMatcherShardsStr = ConfigUtil.getConfig("face_matcher_shards");
        Integer faceMatcherShards = 1;
        try {
            faceMatcherShards = Integer.parseInt(faceMatcherShardsStr);
        } catch (NumberFormatException e) {
            log.error("face_matcher_shards {} is not a number. ", faceMatcherShardsStr, e);
        }

        faceArrayQueue = new ArrayBlockingQueue<>(faceMatcherShards * 2);
        if (tasktypes[0] == 1) {
            Integer finalFaceMatcherShards = faceMatcherShards;
            new Thread(() -> {
                while (true) {
                    List<SrchTaskBean> list = srchTaskDAO.getSrchTaskBean(3, 6, 1, finalFaceMatcherShards);
                    if (list == null || list.size() == 0) {
                        CommonUtil.sleep(interval);
                    } else {
                        for (SrchTaskBean srchTaskBean : list) {
                            try {
                                faceArrayQueue.put(srchTaskBean);
                                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
                            } catch (InterruptedException e) {
                                log.error("Putting into face queue error.", e);
                            }
                        }
                    }
                }
            }, "facett_srchtaskbean_thread").start();
            for (int i = 0; i < faceMatcherShards; i++) {
                new Thread(this::FaceTT, "FaceTT_Thread_" + (i + 1)).start();
            }
        }
    }

    private void FaceTT() {
        while (true) {
            SrchTaskBean srchTaskBean = null;
            try {
                srchTaskBean = faceArrayQueue.take();
            } catch (InterruptedException e) {
                log.error("take srchtaskbean from face Array queue error.", e);
                continue;
            }
//            Blob srchdata = srchTaskBean.getSRCHDATA();
            byte[] srchdata = srchTaskBean.getSRCHDATA();
            int dataType = srchTaskBean.getDATATYPE();
            if (srchdata != null) {
                List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                if (srchDataRecList == null || srchDataRecList.size() <= 0) {
                    log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
                } else {
                    FaceTT(srchDataRecList, srchTaskBean);
                }
            } else {
                log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
            }
        }
    }

    private void FaceTT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        THIDFace.FaceSearchParam probe = new THIDFace.FaceSearchParam();
        FaceTTDAO faceTTDAO = new FaceTTDAO(FaceTT_tablename);
        String tempMsg = srchTaskBean.getEXPTMSG();
        StringBuilder exptMsg;
        if(tempMsg == null){
            exptMsg = new StringBuilder();
        }else{
            exptMsg = new StringBuilder(tempMsg);
        }
        SrchDataRec srchDataRec = srchDataRecList.get(0);
        byte[] feature = srchDataRec.facemnt[0];
        if (feature == null) {
            exptMsg.append("FaceTT feature is null. ");
            log.warn("FaceTT: feature is null. Probeid={}", srchTaskBean.getPROBEID());
        }

        try{

            String dbFilter = CommonUtil.getDBsFilter(srchTaskBean.getSRCHDBSMASK());
            String demoFilter = CommonUtil.getFilter(srchTaskBean.getDEMOFILTER());
            log.info(srchTaskBean.getSRCHDBSMASK());
            probe.filter = CommonUtil.mergeFilter(dbFilter, demoFilter);
            log.info("The total filter is :\n{}", probe.filter);

            probe.scoreThreshold = FaceTT_threshold;
            probe.feature = feature;
            probe.id = srchTaskBean.getPROBEID();
            int numOfCand = srchTaskBean.getNUMOFCAND();
            if(numOfCand > 0){
                probe.maxCands = (int) (numOfCand *1.5);
            }else{
                probe.maxCands = numOfCand = CONSTANTS.MAXCANDS;

            }
            SearchResults<THIDFace.FaceSearchParam.Result> result = HbieUtil.getInstance().hbie_FACE.search(probe);
            List<FaceRec> list = new ArrayList<>();
            for(THIDFace.FaceSearchParam.Result cand:result.candidates){
                FaceRec faceRec = new FaceRec();
                faceRec.taskid = srchTaskBean.getTASKIDD();
                faceRec.transno = srchTaskBean.getTRANSNO();
                faceRec.probeid = srchTaskBean.getPROBEID();
                faceRec.candid = cand.record.id;
                faceRec.dbid = (int) cand.record.info.get("dbId");
                faceRec.score = cand.score;
                faceRec.ffscores[0] = cand.score;
                list.add(faceRec);
            }

            if(list.size() == 0){
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("FaceTT search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("FaceTT search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            }else {
                list = CommonUtil.mergeResult(list);
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                }
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                log.info("begin to write results into {}", FaceTT_tablename);
                boolean isSuc = faceTTDAO.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("FaceTT search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(FaceTT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("FaceTT search results insert into {} error. ProbeId={}", FaceTT_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("FaceTT Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                log.error("FaceTT illegal parameters error. ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString() + e.toString());
            } else {
                log.error("FaceTT exception ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString()+e.toString());
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

    public float getFaceTT_threshold() {
        return FaceTT_threshold;
    }

    public void setFaceTT_threshold(float faceTT_threshold) {
        FaceTT_threshold = faceTT_threshold;
    }

    public String getFaceTT_tablename() {
        return FaceTT_tablename;
    }

    public void setFaceTT_tablename(String faceTT_tablename) {
        FaceTT_tablename = faceTT_tablename;
    }
}
