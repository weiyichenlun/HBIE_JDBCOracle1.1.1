package HAFPIS.service;

import HAFPIS.DAO.PPLLDAO;
import HAFPIS.DAO.PPTLDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.PPLLRec;
import HAFPIS.domain.PPTLRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.MatcherException;
import com.hisign.bie.SearchResults;
import com.hisign.bie.hsfp.HSFPLatPalm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 现场掌纹比对 P2L和L2L
 * Created by ZP on 2017/5/17.
 */
public class LatPalmRecog extends Recog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(LatPalmRecog.class);
    private static ArrayBlockingQueue<SrchTaskBean> pptlArrayQueue;
    private static ArrayBlockingQueue<SrchTaskBean> ppllArrayQueue;
    private float  PPTL_threshold;
    private String PPTL_tablename;
    private float  PPLL_threshold;
    private String PPLL_tablename;


    @Override
    public void run() {
        if (type == CONSTANTS.PPTL) {
            tasktypes[0] = 2;
            datatypes[0] = 2;
        } else if (type == CONSTANTS.PPLL) {
            tasktypes[1] = 4;
            datatypes[1] = 5;
        } else if (type == CONSTANTS.PPTLLL) {
            tasktypes[0] = 2;
            tasktypes[1] = 4;
            datatypes[0] = 2;
            datatypes[1] = 5;
        }
        log.info("Starting...Update status first...");
        srchTaskDAO = new SrchTaskDAO(tablename);
        srchTaskDAO.updateStatus(datatypes, tasktypes);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("----------------");
            boundedExecutor.close();
            srchTaskDAO.updateStatus(datatypes, tasktypes);
            System.out.println("LatPalm executorservice is shutting down");
        }));

        String latpalmMatcherShardsStr = ConfigUtil.getConfig("latpalm_matcher_shards");
        Integer latpalmMatcherShards = 1;
        try {
            latpalmMatcherShards = Integer.parseInt(latpalmMatcherShardsStr);
        } catch (NumberFormatException e) {
            log.error("latpalm_matcher_shards {} is not a number. ", latpalmMatcherShardsStr, e);
        }

        pptlArrayQueue = new ArrayBlockingQueue<>(latpalmMatcherShards * 2);
        ppllArrayQueue = new ArrayBlockingQueue<>(latpalmMatcherShards * 2);

        if (tasktypes[0] == 2) {
            Integer finalLatpalmMatcherShards = latpalmMatcherShards;
            new Thread(() -> {
            while (true) {
                    List<SrchTaskBean> list = srchTaskDAO.getSrchTaskBean(3, 2, 2, finalLatpalmMatcherShards);
                    if (list == null || list.size() == 0) {
                        CommonUtil.sleep(interval);
                    } else {
                        for (SrchTaskBean srchTaskBean : list) {
                            try {
                                pptlArrayQueue.put(srchTaskBean);
                                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
                            } catch (InterruptedException e) {
                                log.error("Putting into pptl queue error.", e);
            }
                        }
                    }
                }
            }, "pptl_srchtaskbean_thread").start();
            for (int i = 0; i < latpalmMatcherShards; i++) {
                new Thread(this::PPTL, "PPTL_Thread_" + (i + 1)).start();
            }
        }
        if (tasktypes[1] == 4) {
            Integer finalLatfpMatcherShards = latpalmMatcherShards;
            new Thread(() -> {
        while (true) {
                    List<SrchTaskBean> list = srchTaskDAO.getSrchTaskBean(3, 5, 4, finalLatfpMatcherShards);
                    if (list == null || list.size() == 0) {
                        CommonUtil.sleep(interval);
                    } else {
                        for (SrchTaskBean srchTaskBean: list) {
                            try {
                                ppllArrayQueue.put(srchTaskBean);
                                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
                            } catch (InterruptedException e) {
                                log.error("Putting into ppll queue error. ", e);
                            }
                        }
                    }
                }
            }, "ppll_srchtaskbean_thread").start();
            for (int i = 0; i < latpalmMatcherShards; i++) {
                new Thread(this::PPLL, "PPLL_Thread_" + (i + 1)).start();
            }
        }
    }

    private void PPTL() {
        while (true) {
            SrchTaskBean srchTaskBean = null;
            try {
                srchTaskBean = pptlArrayQueue.take();
            } catch (InterruptedException e) {
                log.error("take srchtaskbean from pptl Array queue error.", e);
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
                    PPTL(srchDataRecList, srchTaskBean);
                }
                } else {
                    log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                }
        }
    }

    private void PPLL() {
        while (true) {
            SrchTaskBean srchTaskBean = null;
            try {
                srchTaskBean = ppllArrayQueue.take();
            } catch (InterruptedException e) {
                log.error("take srchtaskbean from ppll Array queue error.", e);
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
                    PPLL(srchDataRecList, srchTaskBean);
                }
            } else {
                log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
            }
        }
    }

    private void PPLL(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPLatPalm.LatPalmSearchParam probe = new HSFPLatPalm.LatPalmSearchParam();
        PPLLDAO pplldao = new PPLLDAO(PPLL_tablename);
        String tempMsg = srchTaskBean.getEXPTMSG();
        StringBuilder exptMsg;
        StringBuilder sb = new StringBuilder();
        if (tempMsg == null) {
            exptMsg = new StringBuilder();
        } else {
            exptMsg = new StringBuilder(tempMsg);
        }
        SrchDataRec srchDataRec = srchDataRecList.get(0);
        byte[] feature = srchDataRec.latpalmmnt;
        if (feature == null) {
            exptMsg.append("L2L feature is null");
            log.warn("L2L: feature is null. ProbeId={}", srchTaskBean.getPROBEID());
        }
        long start = 0L;
        try{
            // 比对
            probe.feature = feature;
            probe.id = srchTaskBean.getPROBEID();
            int numOfCand = srchTaskBean.getNUMOFCAND();
            if (numOfCand > 0) {
                probe.maxCands = (int) (numOfCand * 1.5);
            } else {
                probe.maxCands = numOfCand = CONSTANTS.MAXCANDS;
            }

            String dbFilter = CommonUtil.getDBsFilter(srchTaskBean.getSRCHDBSMASK());
            String solveOrDup = CommonUtil.getSolveOrDupFilter(CONSTANTS.DBOP_PLP, srchTaskBean.getSOLVEORDUP());
            log.debug("solveordup filter is {}", solveOrDup);
            String demoFilter = CommonUtil.getFilter(srchTaskBean.getDEMOFILTER());
            log.info(srchTaskBean.getSRCHDBSMASK());
            probe.filter = CommonUtil.mergeFilter(dbFilter, solveOrDup, demoFilter);
            log.info("The total filter is :\n{}", probe.filter);
            SearchResults<HSFPLatPalm.LatPalmSearchParam.Result> results = null;
            start = System.currentTimeMillis();
            results = HbieUtil.getInstance().hbie_PLP.search(probe);
            start = System.currentTimeMillis() - start;
            log.debug("PPLL search cost {} ms", start);
            List<PPLLRec> list = new ArrayList<>();
            for (HSFPLatPalm.LatPalmSearchParam.Result cand : results.candidates) {
                PPLLRec ppllRec = new PPLLRec();
                ppllRec.taskid = srchTaskBean.getTASKIDD();
                ppllRec.transno = srchTaskBean.getTRANSNO();
                ppllRec.probeid = srchTaskBean.getPROBEID();
                ppllRec.candid = cand.record.id;
                ppllRec.dbid = (int) cand.record.info.get("dbId");
                ppllRec.score = cand.score;
                ppllRec.position = 1;
                if (ppllRec.score >= PPLL_threshold) {
                    list.add(ppllRec);
                }
            }
            list = CommonUtil.mergeResult(list);
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("L2L search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("L2L search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                }
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                log.info("begin to write results into {}", PPLL_tablename);
                boolean isSuc = pplldao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("L2L search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(PPLL_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("L2L search results insert into {} error. ProbeId={}", PPLL_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("L2L Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                log.error("L2L illegal parameters error. ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString() + e.toString());
            } else {
                log.error("L2L exception ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString()+e.toString());
            }
        }
    }

    private void PPTL(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPLatPalm.FourPalmSearchParam probe   = new HSFPLatPalm.FourPalmSearchParam();
        PPTLDAO pptldao = new PPTLDAO(PPTL_tablename);
        String tempMsg = srchTaskBean.getEXPTMSG();
        StringBuilder exptMsg;
        String srchPosMask;
        String srchPosMask_Palm;
        int numOfOne = 0;
        int avgCand=0;
        if (tempMsg == null) {
            exptMsg = new StringBuilder();
        } else {
            exptMsg = new StringBuilder(tempMsg);
        }
        srchPosMask = srchTaskBean.getSRCHPOSMASK();
//        if (srchPosMask == null || srchPosMask.length() == 0) {
//            srchPosMask_Palm = "1000110001";
//        } else if (srchPosMask.length() > 0 && srchPosMask.length() <= 10) {
//            char[] tempMask = "0000000000".toCharArray();
//            for (int i = 0; i < 4; i++) {
//                if (srchPosMask.charAt(CONSTANTS.srchOrder[i]) == '1') {
//                    tempMask[CONSTANTS.srchOrder[i]] = '1';
//                }
//            }
//            srchPosMask_Palm = String.valueOf(tempMask);
//        } else {
//            srchPosMask_Palm = srchPosMask.substring(0, 10);
//            if (srchPosMask_Palm.equals("0000000000")) {
//                srchPosMask_Palm = "1000110001";
//            }
//        }
        srchPosMask_Palm = CommonUtil.checkSrchPosMask(CONSTANTS.PPTL, srchPosMask);
        assert srchPosMask_Palm != null;
        SrchDataRec srchDataRec = srchDataRecList.get(0);
        byte[][] features = srchDataRec.palmmnt;
        int[] mask = new int[4];
        for (int i = 0; i < 4; i++) {
            if (srchPosMask_Palm.charAt(CONSTANTS.srchOrder[i]) == '1' && srchDataRec.PalmMntLen[CONSTANTS.srchOrder[i]] != 0) {
                mask[CONSTANTS.feaOrder[i]] = 1;
            }
        }
        numOfOne = srchDataRec.palmmntnum;
        if (srchDataRec.palmmntnum == 0) {
            exptMsg.append(" PalmMnt features are null ");
            log.warn("P2L: PalmMnt features are null. ProbeId={}",srchTaskBean.getPROBEID());
        }
        long start = 0L;
        try{
            SearchResults<HSFPLatPalm.FourPalmSearchParam.Result> results = null;
            List<PPTLRec> list = new ArrayList<>();
            List<PPTLRec> tempList = new ArrayList<>();
            avgCand = srchTaskBean.getAVERAGECAND();
            int numOfCand = srchTaskBean.getNUMOFCAND();
            if (numOfCand > 0) {
                probe.maxCands = numOfCand;
            } else {
                probe.maxCands = CONSTANTS.MAXCANDS;
                numOfCand = CONSTANTS.MAXCANDS;
            }
            int tempCands = numOfCand/numOfOne;
            int tempRes = numOfCand%numOfOne;
            if (tempRes > tempCands / 2) {
                tempCands = tempCands + 1;
            }
            String dbFilter = CommonUtil.getDBsFilter(srchTaskBean.getSRCHDBSMASK());
            String solveOrDup = CommonUtil.getSolveOrDupFilter(CONSTANTS.DBOP_PLP, srchTaskBean.getSOLVEORDUP());
            log.debug("solveordup filter is {}", solveOrDup);
            String demoFilter = CommonUtil.getFilter(srchTaskBean.getDEMOFILTER());
            log.info(srchTaskBean.getSRCHDBSMASK());
            probe.filter = CommonUtil.mergeFilter(dbFilter, solveOrDup, demoFilter);
            log.info("The total filter is :\n{}", probe.filter);
            probe.scoreThreshold = PPTL_threshold;
            probe.id = srchTaskBean.getPROBEID();
            if (avgCand == 1) {
                for (int i = 0; i < mask.length; i++) {
                    if (mask[i] == 1) {
                        probe.feature[i] = features[i];
                        start = System.currentTimeMillis();
                        results = HbieUtil.getInstance().hbie_PLP.search(probe);
                        start = System.currentTimeMillis() - start;
                        log.debug("PPTL search cost {} ms", start);
                        for (int j = 0; j < results.candidates.size(); j++) {
                            HSFPLatPalm.FourPalmSearchParam.Result cand = results.candidates.get(j);
                            PPTLRec pptlRec = new PPTLRec();
                            pptlRec.taskid = srchTaskBean.getTASKIDD();
                            pptlRec.transno = srchTaskBean.getTRANSNO();
                            pptlRec.probeid = srchTaskBean.getPROBEID();
                            pptlRec.candid = cand.record.id;
                            pptlRec.dbid = (int) cand.record.info.get("dbId");
                            pptlRec.score = cand.score;
                            pptlRec.position = cand.outputs[2].galleryPos + 1;
                            if (j < tempCands) {
                                list.add(pptlRec);
                            } else {
                                tempList.add(pptlRec);
                            }
                        }
                    }
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
                probe.feature = features;
                start = System.currentTimeMillis();
                results = HbieUtil.getInstance().hbie_PLP.search(probe);
                start = System.currentTimeMillis() - start;
                log.debug("PPTL search cost {} ms", start);
                for (HSFPLatPalm.FourPalmSearchParam.Result cand : results.candidates) {
                    PPTLRec pptlRec = new PPTLRec();
                    pptlRec.taskid = srchTaskBean.getTASKIDD();
                    pptlRec.transno = srchTaskBean.getTRANSNO();
                    pptlRec.probeid = srchTaskBean.getPROBEID();
                    pptlRec.candid = cand.record.id;
                    pptlRec.dbid = (int) cand.record.info.get("dbId");
                    pptlRec.score = cand.score;
                    pptlRec.position = cand.outputs[2].galleryPos + 1;
                    list.add(pptlRec);
                }
                list = CommonUtil.mergeResult(list);
            }
            // list结果为空的情况
            if ((list == null) || (list.size() == 0)) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("P2L search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString().substring(1, 128));
                }else{
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("P2L search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                }
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                log.info("begin to write results into {}", PPTL_tablename);
                boolean isSuc = pptldao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("P2L search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(PPTL_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("P2L search results insert into {} error. ProbeId={}", PPTL_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("P2L RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("P2L Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
//            log.info("try to restart Matcher...");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                log.error("P2L illegal parameters error. ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString() + e.toString());
            } else {
                log.error("P2L exception ", e);
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

    public float getPPTL_threshold() {
        return PPTL_threshold;
    }

    public void setPPTL_threshold(float PPTL_threshold) {
        this.PPTL_threshold = PPTL_threshold;
    }

    public String getPPTL_tablename() {
        return PPTL_tablename;
    }

    public void setPPTL_tablename(String PPTL_tablename) {
        this.PPTL_tablename = PPTL_tablename;
    }

    public float getPPLL_threshold() {
        return PPLL_threshold;
    }

    public void setPPLL_threshold(float PPLL_threshold) {
        this.PPLL_threshold = PPLL_threshold;
    }

    public String getPPLL_tablename() {
        return PPLL_tablename;
    }

    public void setPPLL_tablename(String PPLL_tablename) {
        this.PPLL_tablename = PPLL_tablename;
    }
}
