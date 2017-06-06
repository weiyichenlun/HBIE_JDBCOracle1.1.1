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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 指纹识别 TT和LT
 * Created by ZP on 2017/5/15.
 */
public class FpRecog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(FpRecog.class);
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private float  FPTT_threshold;
    private String FPTT_tablename;
    private float  FPLT_threshold;
    private String FPLT_tablename;
    private int[] tasktypes = new int[2];
    private int[] datatypes = new int[2];
    private SrchTaskDAO srchTaskDAO;


    @Override
    public void run() {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
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
            List<Future<String>> listF = new ArrayList<>();
            long start1 = System.currentTimeMillis();
            for (int i = 0; i < list.size(); i++) {
                log.debug("in for loop i is  {} ", i);
                srchTaskBean = list.get(i);
                Blob srchdata = srchTaskBean.getSRCHDATA();
                int dataType = srchTaskBean.getDATATYPE();
                if (srchdata != null) {
                    List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                    if (srchDataRecList == null || srchDataRecList.size() <= 0) {
                        log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
                    } else {
                        int tasktype = srchTaskBean.getTASKTYPE();
                        Future<String> future = null;
                        switch (tasktype) {
                            case 1:
                                SrchTaskBean finalSrchTaskBean = srchTaskBean;
                                future = executorService.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        long start = System.currentTimeMillis();
                                        FPTT(srchDataRecList, finalSrchTaskBean);
                                        log.info("FPTT total cost : {} ms", (System.currentTimeMillis() - start));
                                    }
                                }, "FPTT");
//                                long start = System.currentTimeMillis();
//                                FPTT(srchDataRecList, srchTaskBean);
//                                log.info("FPTT total cost : {} ms", (System.currentTimeMillis()-start));
                                break;
                            case 3:
                                SrchTaskBean finalSrchTaskBean1 = srchTaskBean;
                                future = executorService.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        long start1 = System.currentTimeMillis();
                                        FPLT(srchDataRecList, finalSrchTaskBean1);
                                        log.info("FPLT total cost : {} ms", (System.currentTimeMillis() - start1));
                                    }
                                }, "FPLT");
//                                long start1 = System.currentTimeMillis();
//                                FPLT(srchDataRecList, srchTaskBean);
//                                log.info("FPLT total cost : {} ms", (System.currentTimeMillis()-start1));
                                break;
                        }
                        listF.add(future);
                    }
                } else {
                    log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                }
            }
            for (int i = 0; i < listF.size(); i++) {
                Future<String> f = listF.get(i);
                try {
                    f.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            log.info("total cost : {} ms", (System.currentTimeMillis()-start1));
        }
    }

    private void FPLT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPTenFp.LatFpSearchParam probe = new HSFPTenFp.LatFpSearchParam();
        FPLTDAO fpltdao = new FPLTDAO(FPLT_tablename);
        StringBuilder exptMsg ;
        String srchPosMask;
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
        if (srchPosMask == null) {
            srchPosMask="11111111111111111111";
        }
        byte[][] features = new byte[2][];
        SrchDataRec srchDataRec = srchDataRecList.get(0);
        features[0] = srchDataRec.latfpmnt;
        features[1] = null;
        if (features[0] == null) {
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

            SearchResults<HSFPTenFp.LatFpSearchParam.Result> results = null;
            List<FPLTRec> list = new ArrayList<>();
            List<FPLTRec> list_rest = new ArrayList<>();
            for(int i=0; i<10; i++){
                probe.fp_score_weight[i] = 0F;
            }
            //按指位平均输出
            if (avgCand == 1) {
                for (int i = 0; i < posMask_Roll.length; i++) {
                    if (posMask_Roll[i] == 1) {
                        probe.fp_score_weight[i] = posMask_Roll[i];
                        probe.filter = "flag=={0}";
                        results = HbieUtil.getInstance().hbie_FP.search(probe);

                        for (int j = 0; j < results.candidates.size(); j++) {
                            HSFPTenFp.LatFpSearchParam.Result cand = results.candidates.get(j);
                            FPLTRec fpltRec = new FPLTRec();
                            fpltRec.taskid = srchTaskBean.getTASKIDD();
                            fpltRec.transno = srchTaskBean.getTRANSNO();
                            fpltRec.probeid = srchTaskBean.getPROBEID();
                            fpltRec.dbid = 1;
                            fpltRec.candid = cand.record.id;
                            fpltRec.position = cand.fp + 1;
                            fpltRec.score = cand.score;
                            if (results.candidates.size() <= tempCands) {
                                list.add(fpltRec);
                            } else if (j < tempCands && fpltRec.score >= FPLT_threshold) {
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
                        probe.filter = "flag=={1}";
                        results = HbieUtil.getInstance().hbie_FP.search(probe);

                        for (int j = 0; j < results.candidates.size(); j++) {
                            HSFPTenFp.LatFpSearchParam.Result cand = results.candidates.get(j);
                            FPLTRec fpltRec = new FPLTRec();
                            fpltRec.taskid = srchTaskBean.getTASKIDD();
                            fpltRec.transno = srchTaskBean.getTRANSNO();
                            fpltRec.probeid = srchTaskBean.getPROBEID();
                            fpltRec.dbid = 1;
                            fpltRec.candid = cand.record.id;
                            fpltRec.position = cand.fp + 11;
                            fpltRec.score = cand.score;
                            if (results.candidates.size() <= tempCands) {
                                list.add(fpltRec);
                            } else if (j < tempCands && fpltRec.score >= FPLT_threshold) {
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
                for (int i = 0; i < posMask_Roll.length; i++) {
                    probe.fp_score_weight[i] = posMask_Roll[i];
                }
                probe.filter = "flag=={0}";
                results = HbieUtil.getInstance().hbie_FP.search(probe);
                for (HSFPTenFp.LatFpSearchParam.Result cand : results.candidates) {
                    FPLTRec fpltRec = new FPLTRec();
                    fpltRec.taskid = srchTaskBean.getTASKIDD();
                    fpltRec.transno = srchTaskBean.getTRANSNO();
                    fpltRec.probeid = srchTaskBean.getPROBEID();
                    fpltRec.dbid = 1;
                    fpltRec.candid = cand.record.id;
                    fpltRec.position = cand.fp + 1;
                    fpltRec.score = cand.score;
                    if (fpltRec.score > FPLT_threshold) {
                        list.add(fpltRec);
                    }
                }

                for (int i = 0; i < posMask_Flat.length; i++) {
                    probe.fp_score_weight[i] = posMask_Flat[i];
                }
                probe.filter = "flag=={1}";
                results = HbieUtil.getInstance().hbie_FP.search(probe);
                for (HSFPTenFp.LatFpSearchParam.Result cand : results.candidates) {
                    FPLTRec fpltRec = new FPLTRec();
                    fpltRec.taskid = srchTaskBean.getTASKIDD();
                    fpltRec.transno = srchTaskBean.getTRANSNO();
                    fpltRec.probeid = srchTaskBean.getPROBEID();
                    fpltRec.dbid = 1;
                    fpltRec.candid = cand.record.id;
                    fpltRec.position = cand.fp + 11;
                    fpltRec.score = cand.score;
                    if (fpltRec.score > FPLT_threshold) {
                        list.add(fpltRec);
                    }
                }
                list = CommonUtil.mergeResult(list);
            }
            if(list ==null || list.size() ==0){
                if(!exptMsg.toString().isEmpty()){
                    srchTaskBean.setSTATUS(-1);
                    log.error("FPLT search: No results. ProbeId={}, ExceptionMsg:{}",srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString().substring(1, 128));
                }else{
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("FPLT search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            }else {
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
                    log.info("LT search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(FPLT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("LT search results insert into {} error. ProbeId={}", FPLT_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("FPLT Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            log.info("try to restart Matcher...");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        }
    }

    private void FPTT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPTenFp.TenFpSearchParam probe = new HSFPTenFp.TenFpSearchParam();
        FPTTDAO fpttdao = new FPTTDAO(FPTT_tablename);
        String tempMsg = srchTaskBean.getEXPTMSG();
        StringBuilder exptMsg;
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
        try{
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
            probe.filter = "flag=={0}";
            SearchResults<HSFPTenFp.TenFpSearchParam.Result> results = null;
            long start1 = System.currentTimeMillis();
            results = HbieUtil.getInstance().hbie_FP.search(probe);
            for(HSFPTenFp.TenFpSearchParam.Result cand:results.candidates){
                FPTTRec fpttRec = new FPTTRec();
                fpttRec.taskid = srchTaskBean.getTASKIDD();
                fpttRec.transno = srchTaskBean.getTRANSNO();
                fpttRec.probeid = srchTaskBean.getPROBEID();
                fpttRec.dbid = 1;
                fpttRec.candid = cand.record.id;
                fpttRec.rpscores = normalScore(cand.fpscores);
                fpttRec.score = cand.score;
                if (fpttRec.score >= FPTT_threshold) {
                    list.add(fpttRec);
                }
            }
            probe.features = srchDataRec.fpmnt;
            probe.filter = "flag=={1}";
            long start11 = System.currentTimeMillis();
            results = HbieUtil.getInstance().hbie_FP.search(probe);
            long start2 = System.currentTimeMillis();
            log.info("*******In FPTT the saerch time cost is {} ms", (start2-start11));
            for (HSFPTenFp.TenFpSearchParam.Result cand : results.candidates) {
                FPTTRec fpttRec = new FPTTRec();
                fpttRec.taskid = srchTaskBean.getTASKIDD();
                fpttRec.transno = srchTaskBean.getTRANSNO();
                fpttRec.probeid = srchTaskBean.getPROBEID();
                fpttRec.dbid = 1;
                fpttRec.candid = cand.record.id;
                fpttRec.fpscores = normalScore(cand.fpscores);
                fpttRec.score = cand.score;
                if (fpttRec.score >= FPTT_threshold) {
                    list.add(fpttRec);
                }
            }
            list = CommonUtil.mergeResult(list);
            log.info("convert result and merge list cost {} ms", (System.currentTimeMillis()-start2));
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("TT search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("TT search: No results for ProbeId={}", srchTaskBean.getPROBEID());
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
                    log.info("TT search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(FPTT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("TT search results insert into {} error. ProbeId={}", FPTT_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("FPTT Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            log.info("try to restart Matcher...");
//            startTenFpMatcher();
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
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
