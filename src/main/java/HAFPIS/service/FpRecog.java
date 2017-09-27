package HAFPIS.service;

import HAFPIS.DAO.FPLTDAO;
import HAFPIS.DAO.FPTTDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.FPLTRec;
import HAFPIS.domain.FPTTRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.MatcherException;
import com.hisign.bie.SearchResults;
import com.hisign.bie.hsfp.HSFPTenFp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;

/**
 * 指纹识别 TT和LT
 * Created by ZP on 2017/5/15.
 */
public class FpRecog extends Recog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(FpRecog.class);

    private float  FPTT_threshold;
    private String FPTT_tablename;
    private float  FPLT_threshold;
    private String FPLT_tablename;


    @Override
    public void run() {
        if (type == CONSTANTS.FPTT) {
            tasktypes[0] = 1;
            datatypes[0] = 1;
        } else if (type == CONSTANTS.FPLT) {
            datatypes[1] = 4;
            tasktypes[1] = 3;
        } else if (type == CONSTANTS.FPTTLT) {
            tasktypes[0] = 1;
            tasktypes[1] = 3;
            datatypes[0] = 1;
            datatypes[1] = 4;
        }
        srchTaskDAO = new SrchTaskDAO(tablename);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("----------------");
            boundedExecutor.close();
            srchTaskDAO.updateStatus(datatypes, tasktypes);
            System.out.println("Fp executorservice is shutting down");
        }));

        new Thread(()->{
            while (true) {
                CommonUtil.getList(this);
            }
        }, "FP_SrchTaskBean_Thread").start();

        while (true) {
            try{
                SrchTaskBean srchTaskBean = srchTaskBeanArrayBlockingQueue.take();
                Blob srchdata = srchTaskBean.getSRCHDATA();
                int dataType = srchTaskBean.getDATATYPE();
                if (srchdata != null) {
                    List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                    if (srchDataRecList == null || srchDataRecList.size() <= 0) {
                        log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
                    } else {
                        int tasktype = srchTaskBean.getTASKTYPE();
                        switch (tasktype) {
                            case 1:
                                long start = System.currentTimeMillis();
                                try {
                                    boundedExecutor.submitTask(() -> FPTT(srchDataRecList, srchTaskBean));
                                } catch (RejectedExecutionException e) {
                                    log.error("rejected .....", e);
                                }
                                log.debug("FPTT task {} total cost : {} ms", srchTaskBean.getTASKIDD(), (System.currentTimeMillis() - start));
                                break;
                            case 3:
                                long start2 = System.currentTimeMillis();
                                try {
                                    boundedExecutor.submitTask(() -> FPLT(srchDataRecList, srchTaskBean));
                                }  catch (RejectedExecutionException e) {
                                    log.error("rejected .....", e);
                                }
                                log.debug("FPLT task {} total cost : {} ms", srchTaskBean.getTASKIDD(), (System.currentTimeMillis() - start2));
                                break;
                        }
                    }
                } else {
                    log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                }
            } catch (InterruptedException e) {
                log.error("Interrupted during take srchTaskBean from queue");
            }
        }
    }

    private void FPLT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPTenFp.LatFpSearchParam probe = new HSFPTenFp.LatFpSearchParam();
        FPLTDAO fpltdao = new FPLTDAO(FPLT_tablename);
        StringBuilder exptMsg ;
        String srchPosMask;
        StringBuilder sb = new StringBuilder();
        int numOfOne = 0;
        int avgCand=0;
        float[] posMask_Roll = new float[10];
        float[] posMask_Flat = new float[10];

        String tempMsg = srchTaskBean.getEXPTMSG();
        if (tempMsg == null) {
            exptMsg = new StringBuilder();
        } else {
            exptMsg = new StringBuilder(tempMsg);
        }
        srchPosMask = srchTaskBean.getSRCHPOSMASK();
//        if (srchPosMask == null || srchPosMask.length() == 0) {
//            srchPosMask="11111111111111111111";
//        } else if (srchPosMask.length() > 0 && srchPosMask.length() < 20) {
//            char[] tempMask = "00000000000000000000".toCharArray();
//            for (int i = 0; i < srchPosMask.length(); i++) {
//                if (srchPosMask.charAt(i) == '1') {
//                    tempMask[i] = '1';
//                }
//            }
//            srchPosMask = String.valueOf(tempMask);
//        } else {
//            String temp = srchPosMask.substring(0, 20);
//            if (temp.equals("00000000000000000000")) {
//                srchPosMask = "11111111111111111111";
//            }
//        }
        srchPosMask = CommonUtil.checkSrchPosMask(CONSTANTS.FPLT, srchPosMask);
        byte[][] features = new byte[2][];
        SrchDataRec srchDataRec = srchDataRecList.get(0);
        features[0] = srchDataRec.latfpmnt;
        features[1] = srchDataRec.latfpmnt_auto;
        if (features[0] == null && features[1] == null) {
            exptMsg.append(" latfp feature is null");
            log.warn("FPLT: feature is null. probeid=", srchTaskBean.getPROBEID());
        }
        //获取按位比对Mask
        for (int i = 0; i < 10; i++) {
            if (srchPosMask.charAt(i) == '1') {
                posMask_Roll[i] = 1.0F;
                numOfOne = numOfOne + 1;
            }
            if (srchPosMask.charAt(i + 10) == '1') {
                posMask_Flat[i] = 1.0F;
                numOfOne = numOfOne + 1;
            }
        }
        avgCand = srchTaskBean.getAVERAGECAND();
        srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
        try{
            //准备比对
            probe.id = srchTaskBean.getPROBEID();
            probe.feature_manual = features[0];
            if (features[1] != null && features[1].length == 3072) {
                probe.feature_auto = features[1];
            }
            int numOfCand = srchTaskBean.getNUMOFCAND();
            if (numOfCand > 0) {
                probe.maxCands = numOfCand;
            } else {
                numOfCand = CONSTANTS.MAXCANDS;
                probe.maxCands = CONSTANTS.MAXCANDS;
            }
            int tempCands = numOfCand / numOfOne;
            int tempRes = numOfCand % numOfOne;
            if (tempRes > tempCands / 2) {
                tempCands = tempCands + 1;
            }

            String dbFilter = CommonUtil.getDBsFilter(srchTaskBean.getSRCHDBSMASK());
            String solveOrDup = CommonUtil.getSolveOrDupFilter(CONSTANTS.DBOP_TPP, srchTaskBean.getSOLVEORDUP());
            String demoFilter = CommonUtil.getFilter(srchTaskBean.getDEMOFILTER());
            log.info(srchTaskBean.getSRCHDBSMASK());


            SearchResults<HSFPTenFp.LatFpSearchParam.Result> results = null;
            List<FPLTRec> list = new ArrayList<>();
            List<FPLTRec> list_rest = new ArrayList<>();
            for(int i=0; i<10; i++){
                probe.fp_score_weight[i] = 0F;
            }

            probe.scoreThreshold = FPLT_threshold;
            long start = 0;
            //按指位平均输出
            if (avgCand == 1) {
                for (int i = 0; i < posMask_Roll.length; i++) {
                    if (posMask_Roll[i] == 1) {
                        probe.fp_score_weight[i] = posMask_Roll[i];
                        //文字信息过滤
                        probe.filter = CommonUtil.mergeFilter("flag=={0}", dbFilter, solveOrDup, demoFilter);
                        log.info("The total filter is :\n{}", probe.filter);
                        start = System.currentTimeMillis();
                        results = HbieUtil.getInstance().hbie_FP.search(probe);
                        start = System.currentTimeMillis() - start;
                        log.debug("FPLT search cost(flag=={0}) {} ms", start);
                        for (int j = 0; j < results.candidates.size(); j++) {
                            HSFPTenFp.LatFpSearchParam.Result cand = results.candidates.get(j);
                            FPLTRec fpltRec = new FPLTRec();
                            fpltRec.taskid = srchTaskBean.getTASKIDD();
                            fpltRec.transno = srchTaskBean.getTRANSNO();
                            fpltRec.probeid = srchTaskBean.getPROBEID();
                            fpltRec.candid = cand.record.id;
                            fpltRec.dbid = (int) cand.record.info.get("dbId");
                            fpltRec.position = cand.fp + 1;
                            fpltRec.score = cand.score;

                            if (j < tempCands) {
                                list.add(fpltRec);
                            } else {
                                list_rest.add(fpltRec);
                            }
                        }
                        probe.fp_score_weight[i] = 0F;

                    }
                }
                for (int i = 0; i < posMask_Flat.length; i++) {
                    if (posMask_Flat[i] == 1) {
                        probe.fp_score_weight[i] = posMask_Flat[i];
                        sb = new StringBuilder();
                        probe.filter = CommonUtil.mergeFilter("flag=={1}", dbFilter, solveOrDup, demoFilter);
                        log.info("The total filter is :\n{}", probe.filter);
                        start = System.currentTimeMillis();
                        results = HbieUtil.getInstance().hbie_FP.search(probe);
                        start = System.currentTimeMillis() - start;
                        log.debug("FPLT search cost(flag=={1}) {} ms", start);
                        for (int j = 0; j < results.candidates.size(); j++) {
                            HSFPTenFp.LatFpSearchParam.Result cand = results.candidates.get(j);
                            FPLTRec fpltRec = new FPLTRec();
                            fpltRec.taskid = srchTaskBean.getTASKIDD();
                            fpltRec.transno = srchTaskBean.getTRANSNO();
                            fpltRec.probeid = srchTaskBean.getPROBEID();
                            fpltRec.candid = cand.record.id;
                            fpltRec.dbid = (int) cand.record.info.get("dbId");
                            fpltRec.position = cand.fp + 11;
                            fpltRec.score = cand.score;
                            if (j < tempCands) {
                                list.add(fpltRec);
                            } else {
                                list_rest.add(fpltRec);
                            }
                        }
                        probe.fp_score_weight[i] = 0F;
                    }
                }
                list = CommonUtil.mergeResult(list);
                list_rest = CommonUtil.mergeResult(list_rest);
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                } else {
                    list_rest = CommonUtil.getList(list_rest, numOfCand - list.size());
                    list = CommonUtil.mergeResult(list, list_rest);
                }
            } else {
                System.arraycopy(posMask_Roll, 0, probe.fp_score_weight, 0, posMask_Roll.length);
                sb = new StringBuilder();
                probe.filter = CommonUtil.mergeFilter("flag=={0}", dbFilter, solveOrDup, demoFilter);
                log.info("The total filter is :\n{}", probe.filter);
                start = System.currentTimeMillis();
                results = HbieUtil.getInstance().hbie_FP.search(probe);
                start = System.currentTimeMillis() - start;
                log.debug("FPLT search cost(flag=={0}) {} ms", start);
                for (HSFPTenFp.LatFpSearchParam.Result cand : results.candidates) {
                    FPLTRec fpltRec = new FPLTRec();
                    fpltRec.taskid = srchTaskBean.getTASKIDD();
                    fpltRec.transno = srchTaskBean.getTRANSNO();
                    fpltRec.probeid = srchTaskBean.getPROBEID();
                    fpltRec.candid = cand.record.id;
                    fpltRec.dbid = (int) cand.record.info.get("dbId");
                    fpltRec.position = cand.fp + 1;
                    fpltRec.score = cand.score;
                    list.add(fpltRec);
                }

                System.arraycopy(posMask_Flat, 0, probe.fp_score_weight, 0, posMask_Flat.length);
                probe.filter = CommonUtil.mergeFilter("flag=={1}", dbFilter, solveOrDup, demoFilter);
                log.info("The total filter is :\n{}", probe.filter);
                start = System.currentTimeMillis();
                results = HbieUtil.getInstance().hbie_FP.search(probe);
                start = System.currentTimeMillis() - start;
                log.debug("FPLT search cost(flag=={0}) {} ms", start);
                for (HSFPTenFp.LatFpSearchParam.Result cand : results.candidates) {
                    FPLTRec fpltRec = new FPLTRec();
                    fpltRec.taskid = srchTaskBean.getTASKIDD();
                    fpltRec.transno = srchTaskBean.getTRANSNO();
                    fpltRec.probeid = srchTaskBean.getPROBEID();
                    fpltRec.candid = cand.record.id;
                    fpltRec.dbid = (int) cand.record.info.get("dbId");
                    fpltRec.position = cand.fp + 11;
                    fpltRec.score = cand.score;
                    list.add(fpltRec);
                }
                list = CommonUtil.mergeResult(list);
            }
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("FPLT search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString().substring(1, 128));
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("FPLT search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                }
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                log.info("begin to write results into {}", FPLT_tablename);
                boolean isSuc = fpltdao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("FPLT search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(FPLT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("FPLT search results insert into {} error. ProbeId={}", FPLT_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            log.warn("FPLT will reset srch task status = 3");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("FPLT Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            log.info("try to restart Matcher...");
            log.warn("FPLT will reset srch task status = 3");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                log.error("FPLT illegal parameters error. ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString() + e.toString());
            } else {
                log.error("FPLT exception ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString()+e.toString());
            }
        }
    }

    private void FPTT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPTenFp.TenFpSearchParam probe = new HSFPTenFp.TenFpSearchParam();
        FPTTDAO fpttdao = new FPTTDAO(FPTT_tablename);
        String tempMsg = srchTaskBean.getEXPTMSG();
        StringBuilder exptMsg;
        StringBuilder sb = new StringBuilder();
        if (tempMsg == null) {
            exptMsg = new StringBuilder();
        } else {
            exptMsg = new StringBuilder(tempMsg);
        }
        SrchDataRec srchDataRec = srchDataRecList.get(0);
        //判断特征是否为空
        if (srchDataRec.rpmntnum == 0 && srchDataRec.fpmntnum == 0) {
            exptMsg.append(" rollmnt and flatmnt features are both null ");
            log.warn("FPTT: rollmnt and flatmnt features are both null. ProbeId=", srchTaskBean.getPROBEID());
        }
        srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
        try {
            List<FPTTRec> list = new ArrayList<>();
            int numOfCand = srchTaskBean.getNUMOFCAND();
            if (numOfCand > 0) {
                probe.maxCands = (int) (numOfCand * 1.5);
            } else {
                numOfCand = CONSTANTS.MAXCANDS;
                probe.maxCands = CONSTANTS.MAXCANDS;
            }
            probe.id = srchTaskBean.getPROBEID();
            probe.features = srchDataRec.rpmnt;
            probe.scoreThreshold = FPTT_threshold;

            String dbFilter = CommonUtil.getDBsFilter(srchTaskBean.getSRCHDBSMASK());
            log.debug("dbFilter is {}", dbFilter);
            String solveOrDup = CommonUtil.getSolveOrDupFilter(CONSTANTS.DBOP_TPP, srchTaskBean.getSOLVEORDUP());
            log.debug("solveordup filter is {}", solveOrDup);
            String demoFilter = CommonUtil.getFilter(srchTaskBean.getDEMOFILTER());
            log.debug("demofilter is {}", demoFilter);
            log.info(srchTaskBean.getSRCHDBSMASK());

            //文字信息过滤
            probe.filter = CommonUtil.mergeFilter("flag=={0}", dbFilter, solveOrDup, demoFilter);
            log.info("The total filter is :\n{}", probe.filter);

            SearchResults<HSFPTenFp.TenFpSearchParam.Result> results = null;
            long start1 = System.currentTimeMillis();
            results = HbieUtil.getInstance().hbie_FP.search(probe);
            long start2 = System.currentTimeMillis();
//            list = results.candidates.stream().map(result -> {
//                FPTTRec fpttRec = new FPTTRec();
//                fpttRec.taskid = srchTaskBean.getTASKIDD();
//                fpttRec.transno = srchTaskBean.getTRANSNO();
//                fpttRec.probeid = srchTaskBean.getPROBEID();
//                fpttRec.candid = result.record.id;
//                fpttRec.dbid = (int) result.record.info.get("dbId");
//                fpttRec.rpscores = normalScore(result.fpscores);
//                fpttRec.score = result.score;
//                return fpttRec;
//            }).collect(Collectors.toList());

            for (HSFPTenFp.TenFpSearchParam.Result cand : results.candidates) {
                FPTTRec fpttRec = new FPTTRec();
                fpttRec.taskid = srchTaskBean.getTASKIDD();
                fpttRec.transno = srchTaskBean.getTRANSNO();
                fpttRec.probeid = srchTaskBean.getPROBEID();
                fpttRec.candid = cand.record.id;
                fpttRec.dbid = (int) cand.record.info.get("dbId");
                fpttRec.rpscores = normalScore(cand.fpscores);
                fpttRec.score = cand.score;
                list.add(fpttRec);
            }
            log.info("list convertion cost {}", System.currentTimeMillis()-start2);
            probe.features = srchDataRec.fpmnt;
            probe.filter = CommonUtil.mergeFilter("flag=={1}", dbFilter, solveOrDup, demoFilter);
            log.info("The total filter is :\n{}", probe.filter);

            long start11 = System.currentTimeMillis();
            results = HbieUtil.getInstance().hbie_FP.search(probe);
            long start0 = System.currentTimeMillis();
            log.info("*******In FPTT the saerch time cost is {} ms for id {}", (start0 - start11), srchTaskBean.getTASKIDD());
            for (HSFPTenFp.TenFpSearchParam.Result cand : results.candidates) {
                FPTTRec fpttRec = new FPTTRec();
                fpttRec.taskid = srchTaskBean.getTASKIDD();
                fpttRec.transno = srchTaskBean.getTRANSNO();
                fpttRec.probeid = srchTaskBean.getPROBEID();
                fpttRec.candid = cand.record.id;
                fpttRec.dbid = (int) cand.record.info.get("dbId");
                fpttRec.fpscores = normalScore(cand.fpscores);
                fpttRec.score = cand.score;
                list.add(fpttRec);
            }
            list = CommonUtil.mergeResult(list);
            log.info("convert result and merge list cost {} ms", (System.currentTimeMillis() - start0));
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("FPTT search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("FPTT search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                }
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                log.info("begin to write results into {}", FPTT_tablename);
                boolean isSuc = fpttdao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("FPTT search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(FPTT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("FPTT search results insert into {} error. ProbeId={}", FPTT_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            log.warn("FPTT will reset srch task status = 3");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("FPTT Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            log.info("try to restart Matcher...");
//            startTenFpMatcher();
            log.warn("FPTT will reset srch task status = 3");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                log.error("FPTT illegal parameters error. ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString() + e.toString());
            } else {
                log.error("FPTT exception ", e);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString()+e.toString());
            }
        }
    }

    private float[] normalScore(int[] fpscore) {
        float[] res = new float[fpscore.length];
        for(int i=0; i<fpscore.length; i++){
            if(fpscore[i] == -1){
                res[i] = 0;
            }else{
                res[i] = Math.min(1.0F, (float)fpscore[i]/4000.0F);
            }
        }
        return res;
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

    public float getFPTT_threshold() {
        return FPTT_threshold;
    }

    public void setFPTT_threshold(float FPTT_threshold) {
        this.FPTT_threshold = FPTT_threshold;
    }

    public String getFPTT_tablename() {
        return FPTT_tablename;
    }

    public void setFPTT_tablename(String FPTT_tablename) {
        this.FPTT_tablename = FPTT_tablename;
    }

    public float getFPLT_threshold() {
        return FPLT_threshold;
    }

    public void setFPLT_threshold(float FPLT_threshold) {
        this.FPLT_threshold = FPLT_threshold;
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
            log.info("请输入一个配置文件名称(例如HAFPIS_FP.properties):  ");
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
