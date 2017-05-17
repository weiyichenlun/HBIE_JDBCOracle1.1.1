package HAFPIS.service;

import HAFPIS.DAO.FPLLDAO;
import HAFPIS.DAO.FPTLDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.FPLLRec;
import HAFPIS.domain.FPTLRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.MatcherException;
import com.hisign.bie.SearchResults;
import com.hisign.bie.hsfp.HSFPLatFp;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 指纹识别 TL和LL
 * Created by ZP on 2017/5/17.
 */
public class LatFpRecog implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(FpRecog.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private float  FPTL_threshold;
    private String FPTL_tablename;
    private float  FPLL_threshold;
    private String FPLL_tablename;
    int[] tasktypes = new int[2];
    private SrchTaskDAO srchTaskDAO;


    @Override
    public void run() {
        if (type == CONSTANTS.FPTL) {
            tasktypes[0] = 2;
        } else if (type == CONSTANTS.FPLL) {
            tasktypes[1] = 4;
        } else if (type == CONSTANTS.FPTLLL) {
            tasktypes[0] = 2;
            tasktypes[1] = 4;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tablename);
        sb.append(" where status=").append(Integer.parseInt(status));
        sb.append(" and tasktype in (");
        for (int tasktype : tasktypes) {
            if (tasktype != 0) {
                sb.append(tasktype).append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1).append(")");
        sb.append(" and rownum<=").append(Integer.parseInt(queryNum));
        sb.append(" order by priority desc, begtime asc");
        srchTaskDAO = new SrchTaskDAO(tablename);
        while (true) {
            List<SrchTaskBean> list = new ArrayList<>();
            try {
                list = qr.query(sb.toString(), new BeanListHandler<>(SrchTaskBean.class));
                System.out.println(sb.toString());
            } catch (SQLException e) {
                log.error("SQLException: {}, query_sql:{}", e, sb.toString());
            }
            if ((list.size() == 0)) {
                int timeSleep = Integer.parseInt(interval);
                try {
                    Thread.sleep(timeSleep * 10000);
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
                try {
                    if (srchdata != null) {
                        List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                        if (srchDataRecList.size() <= 0) {
                            log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
                        } else {
                            int tasktype = srchTaskBean.getTASKTYPE();
                            switch (tasktype) {
                                case 2:
                                    long start = System.currentTimeMillis();
                                    FPTL(srchDataRecList, srchTaskBean);
                                    log.info("FPTL total cost : {} ms", (System.currentTimeMillis()-start));
                                    break;
                                case 4:
                                    long start1 = System.currentTimeMillis();
                                    FPLL(srchDataRecList, srchTaskBean);
                                    log.info("FPLL total cost : {} ms", (System.currentTimeMillis()-start1));
                                    break;
                            }
                        }
                    } else {
                        log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                        srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                    }
                } catch (Exception e) {

                }
            }
        }
    }

    private void FPTL(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPLatFp.TenFpSearchParam probe = new HSFPLatFp.TenFpSearchParam();
        FPTLDAO fptldao = new FPTLDAO(FPTL_tablename);
        StringBuilder exptMsg ;
        String srchPosMask;
        int numOfOne = 0;
        int avgCand=0;
        float threshold = 0F;
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
        SrchDataRec srchDataRec = srchDataRecList.get(0);
        byte[][] features_roll = srchDataRec.rpmnt;
        byte[][] features_flat = srchDataRec.fpmnt;
        avgCand = srchTaskBean.getAVERAGECAND();

        for(int i=0; i<10;i++) {
            if (srchPosMask.charAt(i) == '1' && srchDataRec.RpMntLen[i] != 0) {
                posMask_Roll[i] = 1.0F;
            }
            if (srchPosMask.charAt(i + 10) == '1' && srchDataRec.FpMntLen[i] != 0) {
                posMask_Flat[i] = 1.0F;
            }
        }
        //判断特征是否为空
        if (srchDataRec.rpmntnum == 0 && srchDataRec.fpmntnum == 0) {
            exptMsg.append("RollMnt and FlatMnt features are both null");
            log.warn("FPTL: RollMnt and FlatMnt features are both null. ProbeId=", srchTaskBean.getPROBEID());
        }
        for(int i=0; i<10; i++) {
            if (posMask_Roll[i] == 1) {
                numOfOne = numOfOne + 1;
            }
            if (posMask_Flat[i] == 1) {
                numOfOne = numOfOne + 1;
            }
        }
        try{
            //设置比对参数
            List<FPTLRec> list_rest = new ArrayList<>();
            List<FPTLRec> list = new ArrayList<>();
            SearchResults<HSFPLatFp.TenFpSearchParam.Result> results = null;
            //设置部分比对
            probe.id = srchTaskBean.getPROBEID();
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
                tempCands = tempCands+1;
            }
            //按指位平均输出
            if (avgCand == 1) {
                for (int i = 0; i < posMask_Roll.length; i++) {
                    if (posMask_Roll[i] == 1) {
                        probe.image = features_roll[i];
//                        probe.filter = "flag=={0}";
                        results = HbieUtil.hbie_LPP.search(probe);

                        for (int j = 0; j < results.candidates.size(); j++) {
                            HSFPLatFp.TenFpSearchParam.Result cand = results.candidates.get(j);
                            FPTLRec fptlRec = new FPTLRec();
                            fptlRec.taskid = srchTaskBean.getTASKIDD();
                            fptlRec.transno = srchTaskBean.getTRANSNO();
                            fptlRec.probeid = srchTaskBean.getPROBEID();
                            fptlRec.dbid = 1;
                            fptlRec.candid = cand.record.id;
                            fptlRec.score = cand.score;
                            fptlRec.position = i + 1;
                            if (results.candidates.size() <= tempCands) {
                                list.add(fptlRec);
                            } else if (j < tempCands && fptlRec.score >= FPTL_threshold) {
                                list.add(fptlRec);
                            } else {
                                list_rest.add(fptlRec);
                            }
                        }
                    }
                }
                for (int i = 0; i < posMask_Flat.length; i++) {
                    if (posMask_Flat[i] == 1) {
                        probe.image = features_flat[i];
                        results = HbieUtil.hbie_LPP.search(probe);
                        for (int j = 0; j < results.candidates.size(); j++) {
                            HSFPLatFp.TenFpSearchParam.Result cand = results.candidates.get(j);
                            FPTLRec fptlRec = new FPTLRec();
                            fptlRec.taskid = srchTaskBean.getTASKIDD();
                            fptlRec.transno = srchTaskBean.getTRANSNO();
                            fptlRec.probeid = srchTaskBean.getPROBEID();
                            fptlRec.dbid = 1;
                            fptlRec.candid = cand.record.id;
                            fptlRec.score = cand.score;
                            fptlRec.position = i + 10 + 1;
                            if (results.candidates.size() < tempCands) {
                                list.add(fptlRec);
                            } else if (j < tempCands && fptlRec.score >= FPTL_threshold) {
                                list.add(fptlRec);
                            } else {
                                list_rest.add(fptlRec);
                            }
                        }
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
                //根据srchPosMash进行比对条件设置
                for (int i = 0; i < posMask_Roll.length; i++) {
                    if (posMask_Roll[i] == 1) {
                        probe.image = features_roll[i];
                        results = HbieUtil.hbie_LPP.search(probe);
                        for (HSFPLatFp.TenFpSearchParam.Result cand : results.candidates) {
                            FPTLRec fptlRec = new FPTLRec();
                            fptlRec.taskid = srchTaskBean.getTASKIDD();
                            fptlRec.transno = srchTaskBean.getTRANSNO();
                            fptlRec.probeid = srchTaskBean.getPROBEID();
                            fptlRec.dbid = 1;
                            fptlRec.candid = cand.record.id;
                            fptlRec.score = cand.score;
                            fptlRec.position = i + 1;
                            if (fptlRec.score > FPTL_threshold) {
                                list.add(fptlRec);
                            }
                        }
                    }
                }

                for (int i = 0; i < posMask_Flat.length; i++) {
                    if (posMask_Flat[i] == 1) {
                        probe.image = features_flat[i];
                        results = HbieUtil.hbie_LPP.search(probe);
                        for (HSFPLatFp.TenFpSearchParam.Result cand : results.candidates) {
                            FPTLRec fptlRec = new FPTLRec();
                            fptlRec.taskid = srchTaskBean.getTASKIDD();
                            fptlRec.transno = srchTaskBean.getTRANSNO();
                            fptlRec.probeid = srchTaskBean.getPROBEID();
                            fptlRec.dbid = 1;
                            fptlRec.candid = cand.record.id;
                            fptlRec.score = cand.score;
                            fptlRec.position = i + 10 + 1;
                            if (fptlRec.score > threshold) {
                                list.add(fptlRec);
                            }
                        }
                    }
                }
                list = CommonUtil.mergeResult(list);
            }
            if(list ==null || list.size() ==0){
                if(!exptMsg.toString().isEmpty()){
                    srchTaskBean.setSTATUS(-1);
                    log.error("FPTL search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString().substring(1, 128));
                }else{
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("FPTL search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            }else{
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                }
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                log.info("begin to write results into {}", FPTL_tablename);
                boolean isSuc = fptldao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("TL search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, null);
                } else {
                    exptMsg.append(FPTL_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("TL search results insert into {} error. ProbeId={}", FPTL_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString().substring(1, 128));
                }
            }
        }  catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString().substring(0,128));
        } catch (MatcherException var7) {
            log.error("FPTL Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            log.info("try to restart Matcher...");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString().substring(0, 128));
        }
    }

    private void FPLL(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPLatFp.LatFpSearchParam probe = new HSFPLatFp.LatFpSearchParam();
        FPLLDAO fplldao = new FPLLDAO(FPLL_tablename);
        StringBuilder exptMsg ;
        String tempMsg = srchTaskBean.getEXPTMSG();
        if (tempMsg == null) {
            exptMsg = new StringBuilder();
        } else {
            exptMsg = new StringBuilder(tempMsg);
        }
        SrchDataRec srchDataRec = srchDataRecList.get(0);
        byte[] feature = srchDataRec.latfpmnt;
        if (feature == null) {
            exptMsg.append("FPLL feature is null. ");
            log.warn("FPLL: feature is null. ProbeId={}", srchTaskBean.getPROBEID());
        }
        try{
            probe.feature_manual = feature;
            probe.id = srchTaskBean.getPROBEID();
            int numOfCand = srchTaskBean.getNUMOFCAND();
            if (numOfCand > 0) {
                probe.maxCands = (int) (numOfCand * 1.5);
            } else {
                probe.maxCands = numOfCand = CONSTANTS.MAXCANDS;
            }

            SearchResults<HSFPLatFp.LatFpSearchParam.Result> results = null;
            results = HbieUtil.hbie_LPP.search(probe);
            List<FPLLRec> list = new ArrayList<>();
            for (HSFPLatFp.LatFpSearchParam.Result cand : results.candidates) {
                FPLLRec fpllRec = new FPLLRec();
                fpllRec.taskid = srchTaskBean.getTASKIDD();
                fpllRec.transno = srchTaskBean.getTRANSNO();
                fpllRec.probeid = srchTaskBean.getPROBEID();
                fpllRec.dbid = 1;
                fpllRec.candid = cand.record.id;
                fpllRec.score = cand.score;
                fpllRec.position = 1;//现场指纹比对默认指位设置为1
                if (fpllRec.score >= FPLL_threshold) {
                    list.add(fpllRec);
                }
            }
            list = CommonUtil.mergeResult(list);
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("LL search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("LL search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                if (list.size() > numOfCand) {
                    list = CommonUtil.getList(list, numOfCand);
                }
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                log.info("begin to write results into {}", FPLL_tablename);
                boolean isSuc = fplldao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("FPLL search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, null);
                } else {
                    exptMsg.append(FPLL_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("FPLL search results insert into {} error. ProbeId={}", FPLL_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString().substring(1, 128));
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString().substring(0,128));
        } catch (MatcherException var7) {
            log.error("FPLL Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            log.info("try to restart Matcher...");
//            startTenFpMatcher();
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString().substring(0, 128));
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

    public float getFPTL_threshold() {
        return FPTL_threshold;
    }

    public void setFPTL_threshold(float FPTL_threshold) {
        this.FPTL_threshold = FPTL_threshold;
    }

    public String getFPTL_tablename() {
        return FPTL_tablename;
    }

    public void setFPTL_tablename(String FPTL_tablename) {
        this.FPTL_tablename = FPTL_tablename;
    }

    public float getFPLL_threshold() {
        return FPLL_threshold;
    }

    public void setFPLL_threshold(float FPLL_threshold) {
        this.FPLL_threshold = FPLL_threshold;
    }

    public String getFPLL_tablename() {
        return FPLL_tablename;
    }

    public void setFPLL_tablename(String FPLL_tablename) {
        this.FPLL_tablename = FPLL_tablename;
    }
}
