package HAFPIS.service;

import HAFPIS.DAO.FPLLDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.FPLLRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.hsfp.HSFPTenFp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 现场指纹1ToF
 * Created by ZP on 2017/5/18.
 */
public class OneToF_FPLL implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(OneToF_FPLL.class);
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private String FPLL_tablename;
    private int[] tasktypes = new int[2];
    private int[] datatypes = new int[2];
    private SrchTaskDAO srchTaskDAO;
    private ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Override
    public void run() {
        srchTaskDAO = new SrchTaskDAO(tablename);
        if (type == CONSTANTS.FPLL1TOF) {
            tasktypes[0] = 8;
            datatypes[0] = 4;
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
            srchTaskDAO.updateStatus(datatypes, tasktypes);
            System.out.println("FPLL1ToF executorservice is shutting down");
        }));
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
                        FPLL(srchDataRecList, srchTaskBean);
                    }
                } else {
                    log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                }
            }
        }

    }

    private void FPLL(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        FPLLDAO fplldao = new FPLLDAO(FPLL_tablename);
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
            List<FPLLRec> list = new ArrayList<>();
            SrchDataRec probe = srchDataRecList.get(0);
            if (probe.latfpmnt == null) {
                exptMsg.append("probe latfpmnt is null.");
                log.error("the probe latfpmnt is null. probeid={}", new String(probe.probeId));
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "probe latfpmnt is null");
            } else {
                Map<String, Future<Float>> map = new HashMap<>();
                List<Future<FPLLRec>> listF = new ArrayList<>();
                for (int i = 1; i < srchDataRecList.size(); i++) {
                    SrchDataRec gallery = srchDataRecList.get(i);
                    if (gallery.latfpmnt == null) {
                        log.warn("gallery latfpmnt is null! the position in the list is {} and probeid is {}", i + 1, new String(gallery.probeId));
                    } else {
                        Future<FPLLRec> rec = executorService.submit(new Callable<FPLLRec>() {
                            @Override
                            public FPLLRec call() throws Exception {
                                FPLLRec fpllRec = new FPLLRec();
                                fpllRec.candid = new String(gallery.probeId).trim();
                                HSFPTenFp.VerifyFeature verifyFeature = new HSFPTenFp.VerifyFeature();
                                verifyFeature.feature1 = probe.latfpmnt;
                                verifyFeature.feature2 = gallery.latfpmnt;
                                HSFPTenFp.VerifyFeature.Result result = HbieUtil.getInstance().hbie_FP.process(verifyFeature);
                                fpllRec.score = result.score;
                                return fpllRec;
                            }
                        });
                        listF.add(rec);
                    }
                }
                for (int i = 0; i < listF.size(); i++) {
                    FPLLRec fpllRec = null;
                    try {
                        fpllRec = listF.get(i).get();
                        fpllRec.taskid = srchTaskBean.getTASKIDD();
                        fpllRec.transno = srchTaskBean.getTRANSNO();
                        fpllRec.probeid = srchTaskBean.getPROBEID();
                        fpllRec.dbid = 0;
                        list.add(fpllRec);
                    } catch (InterruptedException | ExecutionException e) {
                        log.info("FPLL_1ToF get record error, ", e);
                    }
                }
            }
            list = CommonUtil.mergeResult(list);
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("FPLL_1ToF search: No results. ProbeId={}, ExceptionMsg={}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("FPLL_1ToF search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                boolean isSuc = fplldao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("1ToF_FPLL search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(FPLL_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("1ToF_FPLL search results insert into {} error. ProbeId={}", FPLL_tablename, srchTaskBean.getPROBEID());
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

    public String getFPLL_tablename() {
        return FPLL_tablename;
    }

    public void setFPLL_tablename(String FPLL_tablename) {
        this.FPLL_tablename = FPLL_tablename;
    }
}
