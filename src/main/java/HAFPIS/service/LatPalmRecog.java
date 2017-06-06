package HAFPIS.service;

import HAFPIS.DAO.PPLLDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.PPLLRec;
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
                                log.info("P2L total cost : {} ms", (System.currentTimeMillis()-start));
                                break;
                            case 4:
                                long start1 = System.currentTimeMillis();
                                PPLL(srchDataRecList, srchTaskBean);
                                log.info("L2L total cost : {} ms", (System.currentTimeMillis()-start1));
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
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, null);
                } else {
                    exptMsg.append(PPLL_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("L2L search results insert into {} error. ProbeId={}", PPLL_tablename, srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString().substring(1, 128));
                }
            }
        } catch (RemoteException var6) {
            log.error("RemoteExp error: ", var6);
            exptMsg.append("RemoteExp error: ").append(var6);
            srchTaskBean.setEXPTMSG(exptMsg.toString());
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString().substring(0,128));
        } catch (MatcherException var7) {
            log.error("L2L Matcher error: ", var7);
            exptMsg.append("RemoteExp error: ").append(var7);
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), 3, exptMsg.toString().substring(0, 128));
        }
    }

    private void PPTL(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {

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
