package HAFPIS.service;

import HAFPIS.DAO.PPLLDAO;
import HAFPIS.DAO.PPTLDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
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
import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;

/**
 * 现场掌纹比对 P2L和L2L
 * Created by ZP on 2017/5/17.
 */
public class LatPalmRecog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(LatPalmRecog.class);
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private float  PPTL_threshold;
    private String PPTL_tablename;
    private float  PPLL_threshold;
    private String PPLL_tablename;
    private int[] tasktypes = new int[2];
    private int[] datatypes = new int[2];
    private SrchTaskDAO srchTaskDAO;

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
                    Thread.sleep(timeSleep * 10000);
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
                        int tasktype = srchTaskBean.getTASKTYPE();
                        switch (tasktype) {
                            case 2:
                                long start = System.currentTimeMillis();
                                PPTL(srchDataRecList, srchTaskBean);
                                log.debug("P2L total cost : {} ms", (System.currentTimeMillis()-start));
                                break;
                            case 4:
                                long start1 = System.currentTimeMillis();
                                PPLL(srchDataRecList, srchTaskBean);
                                log.debug("L2L total cost : {} ms", (System.currentTimeMillis()-start1));
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

    private void PPLL(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        HSFPLatPalm.LatPalmSearchParam probe = new HSFPLatPalm.LatPalmSearchParam();
        PPLLDAO pplldao = new PPLLDAO(PPLL_tablename);
        String tempMsg = srchTaskBean.getEXPTMSG();
        StringBuilder exptMsg;
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
            SearchResults<HSFPLatPalm.LatPalmSearchParam.Result> results = null;
            results = HbieUtil.getInstance().hbie_PLP.search(probe);
            List<PPLLRec> list = new ArrayList<>();
            for (HSFPLatPalm.LatPalmSearchParam.Result cand : results.candidates) {
                PPLLRec ppllRec = new PPLLRec();
                ppllRec.taskid = srchTaskBean.getTASKIDD();
                ppllRec.transno = srchTaskBean.getTRANSNO();
                ppllRec.probeid = srchTaskBean.getPROBEID();
                ppllRec.dbid = 1;
                ppllRec.candid = cand.record.id;
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
        float threshold = 0F;
        if (tempMsg == null) {
            exptMsg = new StringBuilder();
        } else {
            exptMsg = new StringBuilder(tempMsg);
        }
        srchPosMask = srchTaskBean.getSRCHPOSMASK();
        if (srchPosMask.length() > 10) {
            srchPosMask_Palm = srchPosMask.substring(0, 10);
        }else{
            srchPosMask_Palm = "1000110001";
        }
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
            exptMsg.append(" PalmMnt features is null ");
            log.warn("P2L: PalmMnt features are null. ProbeId=",srchTaskBean.getPROBEID());
        }

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
            probe.id = srchTaskBean.getPROBEID();
            if(avgCand==1){
                for(int i=0; i<mask.length; i++){
                    if(mask[i] == 1){
                        probe.feature[i] = features[i];
                        results = HbieUtil.getInstance().hbie_PLP.search(probe);
                        for(int j=0; j< results.candidates.size(); j++){
                            HSFPLatPalm.FourPalmSearchParam.Result cand = results.candidates.get(j);
                            PPTLRec pptlRec = new PPTLRec();
                            pptlRec.candid = cand.record.id;
                            pptlRec.score  = cand.score;
                            pptlRec.position = cand.outputs[2].galleryPos + 1;
                            if (results.candidates.size() <= tempCands) {
                                list.add(pptlRec);
                            } else {
                                if (j < tempCands && pptlRec.score >= threshold) {
                                    list.add(pptlRec);
                                } else {
                                    tempList.add(pptlRec);
                                }
                            }
                        }
                    }
                }
                tempList = CommonUtil.mergeResult(tempList);
                list = CommonUtil.mergeResult(list);
                if(list.size()>numOfCand){
                    list = CommonUtil.getList(list,numOfCand);
                }else {
                    tempList = CommonUtil.getList(tempList, numOfCand-list.size());
                    list = CommonUtil.mergeResult(list, tempList);
                }
            }else{
                probe.feature = features;
                results = HbieUtil.getInstance().hbie_PLP.search(probe);
                for (HSFPLatPalm.FourPalmSearchParam.Result cand : results.candidates) {
                    PPTLRec pptlRec = new PPTLRec();
                    pptlRec.candid = cand.record.id;
                    pptlRec.score  = cand.score;
                    pptlRec.position = cand.outputs[2].galleryPos + 1;
                    if(pptlRec.score>threshold) {
                        list.add(pptlRec);
                    }
                }
                list = CommonUtil.mergeResult(list);
            }
            // list结果为空的情况
            if ((list == null) || (list.size() == 0)) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("PPTL search: No results. ProbeId={}, ExceptionMsg:{}", srchTaskBean.getPROBEID(), exptMsg);
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
                    log.info("PPTL search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(PPTL_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("PPTL search results insert into {} error. ProbeId={}", PPTL_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                }
            }
        } catch (RemoteException var6) {
            log.error("PPTL RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
        } catch (MatcherException var7) {
            log.error("PPTL Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
//            log.info("try to restart Matcher...");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString());
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
