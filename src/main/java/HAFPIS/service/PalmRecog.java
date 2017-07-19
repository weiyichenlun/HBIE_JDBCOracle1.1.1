package HAFPIS.service;

import HAFPIS.DAO.PPLTDAO;
import HAFPIS.DAO.PPTTDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.PPLTRec;
import HAFPIS.domain.PPTTRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.MatcherException;
import com.hisign.bie.SearchResults;
import com.hisign.bie.hsfp.HSFPFourPalm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 掌纹比对 P2P和L2P
 * Created by ZP on 2017/5/17.
 */
public class PalmRecog implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(PalmRecog.class);
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private float  PPTT_threshold;
    private String PPTT_tablename;
    private float  PPLT_threshold;
    private String PPLT_tablename;
    private int[] tasktypes = new int[2];
    private int[] datatypes = new int[2];
    private SrchTaskDAO srchTaskDAO;
    private ExecutorService executorService = Executors.newFixedThreadPool(CONSTANTS.NCORES);

    @Override
    public void run() {
        if (type == CONSTANTS.PPTT) {
            tasktypes[0] = 1;
            datatypes[0] = 2;
        } else if (type == CONSTANTS.PPLT) {
            tasktypes[1] = 3;
            datatypes[1] = 5;
        } else if (type == CONSTANTS.PPTTLT) {
            tasktypes[0] = 1;
            tasktypes[1] = 3;
            datatypes[0] = 2;
            datatypes[1] = 5;
        }
        srchTaskDAO = new SrchTaskDAO(tablename);
        while (true) {
            List<SrchTaskBean> list;
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
//            SrchTaskBean srchTaskBean = null;
            for (final SrchTaskBean srchTaskBean : list) {
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
                Blob srchdata = srchTaskBean.getSRCHDATA();
                int dataType = srchTaskBean.getDATATYPE();
                if (srchdata != null) {
                    List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                    if (srchDataRecList == null || srchDataRecList.size() <= 0) {
                        log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
                        srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata format error");
                    } else {
                        int tasktype = srchTaskBean.getTASKTYPE();
                        switch (tasktype) {
                            case 1:
                                long start = System.currentTimeMillis();
//                                PPTT(srchDataRecList, srchTaskBean);
                                executorService.submit(() -> PPTT(srchDataRecList, srchTaskBean));
                                log.debug("PPTT total cost : {} ms", (System.currentTimeMillis() - start));
                                break;
                            case 3:
                                long start1 = System.currentTimeMillis();
//                                PPLT(srchDataRecList, srchTaskBean);
                                executorService.submit(() -> PPLT(srchDataRecList, srchTaskBean));
                                log.debug("PPLT total cost : {} ms", (System.currentTimeMillis() - start1));
                                break;
                        }
                    }
                } else {
                    log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                }
            }
        }
    }

    private void PPLT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPFourPalm.LatPalmSearchParam probe   = new HSFPFourPalm.LatPalmSearchParam();
        PPLTDAO ppltdao = new PPLTDAO(PPLT_tablename);
//        String srchPosMask="10001100011111111111";
        String srchPosMask_Palm;
        StringBuilder exptMsg;
        StringBuilder sb = new StringBuilder();
        int numOfOne = 0;
        int avgCand=0;
        String tempMsg = srchTaskBean.getEXPTMSG();
        if (tempMsg == null) {
            exptMsg = new StringBuilder();
        } else {
            exptMsg = new StringBuilder(tempMsg);
        }
        SrchDataRec srchDataRec = srchDataRecList.get(0);
        String srchPosMask = srchTaskBean.getSRCHPOSMASK();
        if (srchPosMask == null || srchPosMask.length() < 10) {
            srchPosMask_Palm = "1000110001";
        } else {
            srchPosMask_Palm = srchPosMask.substring(0, 10);
        }
        boolean[] mask = new boolean[4];
        for (int i = 0; i < 4; i++) {
            if (srchPosMask_Palm.charAt(CONSTANTS.srchOrder[i]) == '1') {
                mask[CONSTANTS.feaOrder[i]] = true;
            }
        }

        byte[] feature = srchDataRec.latpalmmnt;
        if (feature == null) {
            exptMsg.append("L2P feature is null");
            log.warn("L2P: feature is null. ProbeId={}",srchTaskBean.getPROBEID());
        }
        //根据srchPosMash进行比对条件设置
        for (boolean aMask : mask) {
            if (aMask) {
                numOfOne = numOfOne + 1;
            }
        }
        avgCand = srchTaskBean.getAVERAGECAND();
        int numOfCand = srchTaskBean.getNUMOFCAND();
        if (numOfCand > 0) {
            probe.maxCands = numOfCand;
        } else {
            probe.maxCands = numOfCand = CONSTANTS.MAXCANDS;
        }
        int tempCands = numOfCand / numOfOne;
        int tempRes = numOfCand % numOfOne;
        if (tempRes > tempCands / 2) {
            tempCands = tempCands + 1;
        }
        for (int i = 0; i < mask.length; i++) {
            probe.ppMask[i] = false;
        }

        SearchResults<HSFPFourPalm.LatPalmSearchParam.Result> results = null;
        List<PPLTRec> list = new ArrayList<>();
        List<PPLTRec> tempList = new ArrayList<>();
        try{
            // 比对参数设置
            probe.id      = srchTaskBean.getPROBEID();
            probe.feature = feature;

            String dbFilter = CommonUtil.getDBsFilter(srchTaskBean.getSRCHDBSMASK());
            String demoFilter = CommonUtil.getFilter(srchTaskBean.getDEMOFILTER());
            if (null == demoFilter || demoFilter.trim().isEmpty()) {
            } else {
                sb.append(" && ").append(demoFilter);
            }
            if (null == dbFilter || dbFilter.trim().isEmpty()) {
            } else {
                sb.append(" && ").append(dbFilter);
            }
            if (sb.toString().length() > 2) {
                sb.replace(0, 2, "");
            }
            System.out.println(sb.toString());

            probe.filter = sb.toString();

//            probe.recordAllScores = true;
            if (avgCand == 1) {
                for (int i = 0; i < mask.length; i++) {
                    if (mask[i]) {
                        probe.ppMask[i] = true;
                        results = HbieUtil.getInstance().hbie_PP.search(probe);
                        for (int j = 0; j < results.candidates.size(); j++) {
                            HSFPFourPalm.LatPalmSearchParam.Result cand = results.candidates.get(j);
                            PPLTRec ppltRec = new PPLTRec();
                            ppltRec.taskid = srchTaskBean.getTASKIDD();
                            ppltRec.transno = srchTaskBean.getTRANSNO();
                            ppltRec.probeid = srchTaskBean.getPROBEID();
                            ppltRec.candid = cand.record.id;
                            ppltRec.dbid = (int) cand.record.info.get("dbId");
                            ppltRec.position = cand.outputs[2].galleryPos;
                            ppltRec.score = cand.score;
                            if (results.candidates.size() <= PPLT_threshold) {
                                list.add(ppltRec);
                            } else if (j < tempCands && ppltRec.score >= PPLT_threshold) {
                                list.add(ppltRec);
                            } else {
                                tempList.add(ppltRec);
                            }
                        }
                    }
                    probe.ppMask[i] = false;
                }
                tempList = CommonUtil.mergeResult(tempList);
                list = CommonUtil.mergeResult(list);
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                } else {
                    tempList = CommonUtil.getList(tempList, numOfCand - list.size());
                    list = CommonUtil.mergeResult(list, tempList);
                }
            } else {
                for (int i = 0; i < mask.length; i++) {
                    if (mask[i]) {
                        probe.ppMask[i] = true;
                    }
                }
                results = HbieUtil.getInstance().hbie_PP.search(probe);
                for (HSFPFourPalm.LatPalmSearchParam.Result cand : results.candidates) {
                    PPLTRec ppltRec = new PPLTRec();
                    ppltRec.taskid = srchTaskBean.getTASKIDD();
                    ppltRec.transno = srchTaskBean.getTRANSNO();
                    ppltRec.probeid = srchTaskBean.getPROBEID();
                    ppltRec.candid = cand.record.id;
                    ppltRec.dbid = (int) cand.record.info.get("dbId");
                    ppltRec.position = cand.outputs[2].galleryPos;
                    ppltRec.score = cand.score;
                    if (ppltRec.score >= PPLT_threshold) {
                        list.add(ppltRec);
                    }
                }
                list = CommonUtil.mergeResult(list);
            }
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("L2P search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString().substring(1, 128));
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("L2P search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                }
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                log.info("begin to write results into {}", PPLT_tablename);
                boolean isSuc = ppltdao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("L2P search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(PPLT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("L2P search results insert into {} error. ProbeId={}", PPLT_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("L2P Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                log.error("L2P illegal parameters error. ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString() + e.toString());
            } else {
                log.error("L2P exception ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString()+e.toString());
            }
        }
    }

    private void PPTT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPFourPalm.FourPalmSearchParam probe   = new HSFPFourPalm.FourPalmSearchParam();
        PPTTDAO ppttdao = new PPTTDAO(PPTT_tablename);
        StringBuilder exptMsg;
        StringBuilder sb = new StringBuilder();
        String tempMsg = srchTaskBean.getEXPTMSG();
        if (tempMsg == null) {
            exptMsg = new StringBuilder();
        } else {
            exptMsg = new StringBuilder(tempMsg);
        }

        SrchDataRec srchDataRec = srchDataRecList.get(0);
        byte[][] features = srchDataRec.palmmnt;
        if (srchDataRec.palmmntnum == 0) {
            exptMsg.append("P2P features are null");
            log.warn("P2P: features are null. ProbeId={}", srchTaskBean.getPROBEID());
        }
        try{
            List<PPTTRec> list = new ArrayList<>();
            probe.features = features;
            probe.id       = srchTaskBean.getPROBEID();
            int numOfCand = srchTaskBean.getNUMOFCAND();
            if (numOfCand > 0) {
                probe.maxCands = (int) (numOfCand * 1.5);
            } else {
                probe.maxCands = numOfCand = CONSTANTS.MAXCANDS;
            }

            String dbFilter = CommonUtil.getDBsFilter(srchTaskBean.getSRCHDBSMASK());
            String demoFilter = CommonUtil.getFilter(srchTaskBean.getDEMOFILTER());
            //文字信息过滤
            if (null == demoFilter || demoFilter.trim().isEmpty()) {
            } else {
                sb.append(" && ").append(demoFilter);
            }
            if (null == dbFilter || dbFilter.trim().isEmpty()) {
            } else {
                sb.append(" && ").append(dbFilter);
            }
            if (sb.toString().length() > 2) {
                sb.replace(0, 2, "");
            }
            System.out.println(sb.toString());

            probe.filter = sb.toString();

            SearchResults<HSFPFourPalm.FourPalmSearchParam.Result> results = HbieUtil.getInstance().hbie_PP.search(probe);
            for (HSFPFourPalm.FourPalmSearchParam.Result cand : results.candidates) {
                PPTTRec ppttRec = new PPTTRec();
                ppttRec.taskid = srchTaskBean.getTASKIDD();
                ppttRec.transno = srchTaskBean.getTRANSNO();
                ppttRec.probeid = srchTaskBean.getPROBEID();
                ppttRec.candid = cand.record.id;
                ppttRec.dbid = (int) cand.record.info.get("dbId");
                ppttRec.score  = cand.score;
                ppttRec.position = cand.outputs[2].galleryPos;
                if (ppttRec.score > PPTT_threshold) {
                    list.add(ppttRec);
                }
            }

            if (list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("P2P search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("P2P search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                }
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                log.info("begin to write results into {}", PPTT_tablename);
                boolean isSuc = ppttdao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("P2P search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(PPTT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("P2P search results insert into {} error. ProbeId={}", PPTT_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("P2P Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                log.error("P2P illegal parameters error. ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString() + e.toString());
            } else {
                log.error("P2P exception ", e);
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

    public float getPPTT_threshold() {
        return PPTT_threshold;
    }

    public void setPPTT_threshold(float PPTT_threshold) {
        this.PPTT_threshold = PPTT_threshold;
    }

    public String getPPTT_tablename() {
        return PPTT_tablename;
    }

    public void setPPTT_tablename(String PPTT_tablename) {
        this.PPTT_tablename = PPTT_tablename;
    }

    public String getPPLT_tablename() {
        return PPLT_tablename;
    }

    public void setPPLT_tablename(String PPLT_tablename) {
        this.PPLT_tablename = PPLT_tablename;
    }

    public float getPPLT_threshold() {
        return PPLT_threshold;
    }

    public void setPPLT_threshold(float PPLT_threshold) {
        this.PPLT_threshold = PPLT_threshold;
    }
}
