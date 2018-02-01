package HAFPIS.service;

import HAFPIS.DAO.PPLLDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.PPLLRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.hsfp.HSFPFourPalm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 现场掌纹1比F实现
 * Created by ZP on 2017/6/5.
 */
public class OneToF_PPLL extends Recog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(OneToF_PPLL.class);

    private String PPLL_tablename;

    ExecutorService executorService = Executors.newFixedThreadPool(5);


    @Override
    public void run() {
        srchTaskDAO = new SrchTaskDAO(tablename);
        if (type == CONSTANTS.PPLL1TOF) {
            tasktypes[0] = 8;
            datatypes[0] = 5;
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
            System.out.println("PPLL1ToF executorservice is shutting down");
        }));
        while (true) {
            List<SrchTaskBean> list = new ArrayList<>();
            list = srchTaskDAO.getList(status, datatypes, tasktypes, queryNum);
            CommonUtil.checkList(list, interval);
            SrchTaskBean srchTaskBean = null;
            for (int i = 0; i < list.size(); i++) {
                srchTaskBean = list.get(i);
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
//                Blob srchdata = srchTaskBean.getSRCHDATA();
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
    }

    private void PPLL(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        PPLLDAO pplldao = new PPLLDAO(PPLL_tablename);
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
            log.error("there is only one SrchDataRec in srchDataRecList, PPLL_1ToF will stop");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "only one srchdata record");
        } else {
            List<PPLLRec> list = new ArrayList<>();
            SrchDataRec probe = srchDataRecList.get(0);
            if (probe.latfpmnt == null) {
                exptMsg.append("probe latfpmnt is null.");
                log.error("the probe latfpmnt is null. probeid={}", new String(probe.probeId));
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "probe latfpmnt is null");
            } else {
                List<Future<PPLLRec>> listF = new ArrayList<>();
                for (int i = 1; i < srchDataRecList.size(); i++) {
                    SrchDataRec gallery = srchDataRecList.get(i);
                    if (gallery.latfpmnt == null) {
                        log.warn("gallery latfpmnt is null! the position in the list is {} and probeid is {}", i + 1, new String(gallery.probeId));
                    } else {
                        Future<PPLLRec> rec = executorService.submit(new Callable<PPLLRec>() {
                            @Override
                            public PPLLRec call() throws Exception {
                                PPLLRec fpllRec = new PPLLRec();
                                fpllRec.candid = new String(gallery.probeId).trim();
                                HSFPFourPalm.VerifyFeature verifyFeature = new HSFPFourPalm.VerifyFeature();
                                verifyFeature.feature1[2] = probe.latpalmmnt;
                                verifyFeature.feature2[2] = gallery.latpalmmnt;
                                HSFPFourPalm.VerifyFeature.Result result = HbieUtil.getInstance().hbie_PP.process(verifyFeature);
                                fpllRec.score = result.score;
                                return fpllRec;
                            }
                        });
                        listF.add(rec);
                    }
                }
                for (int i = 0; i < listF.size(); i++) {
                    PPLLRec ppllRec = null;
                    try {
                        ppllRec = listF.get(i).get();
                        ppllRec.taskid = srchTaskBean.getTASKIDD();
                        ppllRec.transno = srchTaskBean.getTRANSNO();
                        ppllRec.probeid = srchTaskBean.getPROBEID();
                        ppllRec.dbid = 0;
                        list.add(ppllRec);
                    } catch (InterruptedException | ExecutionException e) {
                        log.info("PPLL_1ToF get record error, ", e);
                    }
                }
            }
            list = CommonUtil.mergeResult(list);
            if (list == null || list.size() == 0) {
                if (!exptMsg.toString().isEmpty()) {
                    srchTaskBean.setSTATUS(-1);
                    log.error("PPLL_1ToF search: No results. ProbeId={}, ExceptionMsg={}", srchTaskBean.getPROBEID(), exptMsg);
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
                } else {
                    srchTaskBean.setEXPTMSG("No results");
                    srchTaskBean.setSTATUS(6);
                    log.info("PPLL_1ToF search: No results for ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 6, "no results");
                }
            } else {
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).candrank = i + 1;
                }
                boolean isSuc = pplldao.updateRes(list);
                if (isSuc) {
                    srchTaskBean.setSTATUS(5);
                    log.info("1ToF_PPLL search finished. ProbeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
                } else {
                    exptMsg.append(PPLL_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                    log.error("1ToF_PPLL search results insert into {} error. ProbeId={}", PPLL_tablename, srchTaskBean.getPROBEID());
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

    public String getPPLL_tablename() {
        return PPLL_tablename;
    }

    public void setPPLL_tablename(String PPLL_tablename) {
        this.PPLL_tablename = PPLL_tablename;
    }
}
