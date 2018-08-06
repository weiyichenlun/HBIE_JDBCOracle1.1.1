package HAFPIS.service;

import HAFPIS.DAO.FPTTDAO;
import HAFPIS.DAO.HeartBeatDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.FPTTRec;
import HAFPIS.domain.HeartBeatBean;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 指纹1ToF比对
 * Created by ZP on 2017/5/18.
 */
public class OneToF_FPTT extends Recog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(OneToF_FPTT.class);

    private String FPTT_tablename;

    private ExecutorService executorService = Executors.newFixedThreadPool(5);
    @Override
    public void run() {
        String heartBeatTable = ConfigUtil.getConfig("heart_beat_table");
        String instanceName = ConfigUtil.getConfig("instance_name");
        if (heartBeatTable == null || instanceName == null) {
            log.warn("No heartbeat config found. ");
        } else {
            CommonUtil.sleep("" + CONSTANTS.SLEEP_TIME);
            heartBeatDAO = new HeartBeatDAO(heartBeatTable);
            while (true) {
                HeartBeatBean bean = heartBeatDAO.queryLatest();
                if (bean == null) {
                    try {
                        Thread.sleep(3 * 1000);
                        continue;
                    } catch (InterruptedException e) {
                    }
                }
                if (bean.getINSTANCENAME().equals(instanceName) && bean.getUPDATETIME() > 0) {
                    log.info("Current active instance is {}, this instance is {}", bean.getINSTANCENAME(), instanceName);
                    break;
                } else if (!bean.getINSTANCENAME().equals(instanceName)) {
                    log.debug("Current active instance: {}, but this instance is {}", bean.getINSTANCENAME(), instanceName);
                    try {
                        Thread.sleep(3 * 1000);
                    } catch (InterruptedException e) {
                        log.error("Error. ", e);
                    }
                }
            }
        }
        srchTaskDAO = new SrchTaskDAO(tablename);
        if (type == CONSTANTS.FPTT1TOF) {
            tasktypes[0] = 8;
            datatypes[0] = 1;
        } else {
            log.warn("FPTT_1ToF the type is wrong. type={}", type);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("----------------");
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            executorService.shutdown();
            while (true) {
                try {
                    srchTaskDAO.updateStatus(datatypes, tasktypes);
                    break;
                } catch (Exception e) {
                    log.error("database error. ", e);
                    CommonUtil.sleep("10");
                    continue;
                }
            }
            System.out.println("FPTT1ToF executorservice is shutting down");
        }));
        while (true) {
            List<SrchTaskBean> list = new ArrayList<>();
            try {
                list = srchTaskDAO.getList(status, datatypes, tasktypes, queryNum);
            } catch (Exception e) {
                log.error("1tof fptt error. ", e);
                CommonUtil.sleep("10");
                continue;
            }
            CommonUtil.checkList(list, interval);
            SrchTaskBean srchTaskBean = null;
            for (int i = 0; i < list.size(); i++) {
                srchTaskBean = list.get(i);
//                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
                Blob srchdata = srchTaskBean.getSRCHDATA();
//                byte[] srchdata = srchTaskBean.getSRCHDATA();
                int dataType = srchTaskBean.getDATATYPE();
                if (srchdata != null) {
                    List<SrchDataRec> srchDataRecList = CommonUtil.srchdata2Rec(srchdata, dataType);
                    if (srchDataRecList == null || srchDataRecList.size() <= 0) {
                        log.error("can not get srchdatarec from srchdata for probeid={}", srchTaskBean.getPROBEID());
                        srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "can not get srchdata");
                    } else {
                        FPTT(srchDataRecList, srchTaskBean);
                    }
                } else {
                    log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                }
            }
        }

    }

    private void FPTT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        FPTTDAO fpttdao = new FPTTDAO(FPTT_tablename);
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
            log.error("there is only one SrchDataRec in srchDataRecList, FPTT_1ToF will stop");
            srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "only one srchdata record");
        } else {
            List<FPTTRec> list = new ArrayList<>();
            SrchDataRec probe = srchDataRecList.get(0);
            for (int i = 1; i < srchDataRecList.size(); i++) {
                SrchDataRec gallery = srchDataRecList.get(i);
                if (gallery.rpmntnum == 0 && gallery.fpmntnum == 0) {
                    exptMsg.append("list.get(").append(i).append(")\'s rollmnt and flatmnt are both null").append(" probeid is").append(new String(gallery.probeId));
                    log.warn("1ToF rollmnt and flatmnt are both null for the record in list.get({}), probeid={}.", i, new String(gallery.probeId));
                }else {
                    Map<Integer, Future<Float>> map = new HashMap<>();
                    FPTTRec fpttRec = new FPTTRec();
                    fpttRec.taskid = srchTaskBean.getTASKIDD();
                    fpttRec.transno = srchTaskBean.getTRANSNO();
                    fpttRec.probeid = srchTaskBean.getPROBEID();
                    fpttRec.dbid = 0;
                    fpttRec.candid = new String(gallery.probeId).trim();
                    int len = gallery.rpmntnum;
                    if (len > 0) {
                        for (int j = 0; j < 10; j++) {
                            final int finalJ = j;
                            Future<Float> score = executorService.submit(new Callable<Float>() {
                                @Override
                                public Float call() throws Exception {
                                    HSFPTenFp.VerifyFeature verifyFuture = new HSFPTenFp.VerifyFeature();
                                    verifyFuture.feature1 = probe.rpmnt[finalJ];
                                    verifyFuture.feature2 = gallery.rpmnt[finalJ];
                                    HSFPTenFp.VerifyFeature.Result result = HbieUtil.getInstance().hbie_FP.process(verifyFuture);
                                    return result.score;
                                }
                            });
                            map.put(j, score);
                        }
                    }
                    len = gallery.fpmntnum;
                    if (len > 0) {
                        for (int j = 0; j < 10; j++) {
                            final int finalJ = j;
                            Future<Float> score = executorService.submit(new Callable<Float>() {
                                @Override
                                public Float call() throws Exception {
                                    HSFPTenFp.VerifyFeature verifyFuture = new HSFPTenFp.VerifyFeature();
                                    verifyFuture.feature1 = probe.fpmnt[finalJ];
                                    verifyFuture.feature2 = gallery.fpmnt[finalJ];
                                    HSFPTenFp.VerifyFeature.Result result = HbieUtil.getInstance().hbie_FP.process(verifyFuture);
                                    return result.score;
                                }
                            });
                            map.put(j + 10, score);
                        }
                    }
                    float tempScore = 0F;
                    for (int j = 0; j < map.size(); j++) {
                        Future<Float> f = map.get(j);
                        float temp = 0F;
                        try {
                            temp = f.get();
                        } catch (InterruptedException | ExecutionException e) {
                            log.info("get 1ToF score map error, probeid={} ", new String(gallery.probeId), e);
                            continue;
                        }
                        if (temp > tempScore) {
                            tempScore = temp;
                        }
                        if (j >= 0 && j < 10) {
                            fpttRec.rpscores[j] = (int) temp;
                        } else {
                            fpttRec.fpscores[j - 10] = (int) temp;
                        }
                    }
                    fpttRec.score = tempScore;
                    list.add(fpttRec);
                }
            }
            list = CommonUtil.sort(list);
            for (int i = 0; i < list.size(); i++) {
                list.get(i).candrank = i + 1;
            }
            boolean isSuc = fpttdao.updateRes(list);
            if (isSuc) {
                srchTaskBean.setSTATUS(5);
                log.info("1ToF_FPTT search finished. ProbeId={}", srchTaskBean.getPROBEID());
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
            } else {
                exptMsg.append(FPTT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                log.error("1ToF_FPTT search results insert into {} error. ProbeId={}", FPTT_tablename, srchTaskBean.getPROBEID());
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString());
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

    public String getFPTT_tablename() {
        return FPTT_tablename;
    }

    public void setFPTT_tablename(String FPTT_tablename) {
        this.FPTT_tablename = FPTT_tablename;
    }
}
