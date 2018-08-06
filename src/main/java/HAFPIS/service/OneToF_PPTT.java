package HAFPIS.service;

import HAFPIS.DAO.HeartBeatDAO;
import HAFPIS.DAO.PPTTDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.HeartBeatBean;
import HAFPIS.domain.PPTTRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.hsfp.HSFPFourPalm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 *
 * Created by ZP on 2017/5/19.
 */
public class OneToF_PPTT extends Recog implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(OneToF_PPTT.class);

    private String PPTT_tablename;
    
    ExecutorService executorService = Executors.newFixedThreadPool(5);

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
        if (type == CONSTANTS.PPTT1TOF) {
            tasktypes[0] = 8;
            datatypes[0] = 2;
        } else {
            log.warn("PPTT_1ToF the type is wrong. type={}", type);
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
                } catch (SQLException e) {
                    log.error("database error. ", e);
                    CommonUtil.sleep("10");
                    continue;
                }
            }
            System.out.println("PPTT1ToF executorservice is shutting down");
        }));
        while (true) {
            List<SrchTaskBean> list = new ArrayList<>();
            try {
                list = srchTaskDAO.getList(status, datatypes, tasktypes, queryNum);
            } catch (SQLException e) {
                log.error("1tof pptt database error");
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
                        PPTT(srchDataRecList, srchTaskBean);
                    }
                } else {
                    log.warn("srchdata is null for probeId={}", srchTaskBean.getPROBEID());
                    srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "srchdata is null");
                }
            }
        }
    }

    private void PPTT(List<SrchDataRec> srchDataRecList, SrchTaskBean srchTaskBean) {
        PPTTDAO ppttdao = new PPTTDAO(PPTT_tablename);
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
            List<PPTTRec> list = new ArrayList<>();
            SrchDataRec probe = srchDataRecList.get(0);
            if (probe.palmmntnum == 0) {
                exptMsg.append("probe palmmnt is null");
                log.error("probe palmmnt is null. probeid={}", new String(probe.probeId));
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, "probe palmmnt is null");
            } else{
                for (int i = 1; i < srchDataRecList.size(); i++) {
                    SrchDataRec gallery = srchDataRecList.get(i);
                    if (gallery.palmmntnum == 0) {
                        exptMsg.append("gallery (").append(i).append(") palmmnt is null. probeid is").append(new String(gallery.probeId));
                        log.warn("gallery ({}) palmmnt is null. probeid is {}", i, new String(gallery.probeId));
                    } else {
                        Map<Integer, Future<Float>> map = new HashMap<>();
                        PPTTRec ppttRec = new PPTTRec();
                        ppttRec.taskid = srchTaskBean.getTASKIDD();
                        ppttRec.transno = srchTaskBean.getTRANSNO();
                        ppttRec.probeid = srchTaskBean.getPROBEID();
                        ppttRec.dbid = 0;
                        ppttRec.candid = new String(gallery.probeId).trim();
                        byte[][] feature1 = new byte[4][];
                        byte[][] feature2 = new byte[4][];
                        for (int j = 0; j < 4; j++) {
                            feature1[j] = probe.palmmnt[j];
                            feature2[j] = gallery.palmmnt[j];
                            Future<Float> score = executorService.submit(() -> {
                                HSFPFourPalm.VerifyFeature verifyFeature = new HSFPFourPalm.VerifyFeature();
                                verifyFeature.feature1 = feature1;
                                verifyFeature.feature2 = feature2;
                                HSFPFourPalm.VerifyFeature.Result result = HbieUtil.getInstance().hbie_PP.process(verifyFeature);
                                return result.score;
                            });
                            map.put(j, score);
                        }
                        float tempScore = 0F;
                        for (int j = 0; j < map.size(); j++) {
                            Future<Float> f = map.get(j);
                            float temp = 0F;
                            try {
                                temp = f.get();
                            } catch (InterruptedException | ExecutionException e) {
                                log.info("get 1ToF score map error, probeid={} ", new String(gallery.probeId), e);
                            }
                            if (temp > tempScore) {
                                tempScore = temp;
                            }
                            ppttRec.ppscores[j] = temp;
                        }
                        ppttRec.score = tempScore;
//                        HSFPFourPalm.VerifyFeature verifyFeature = new HSFPFourPalm.VerifyFeature();
//                        verifyFeature.feature1 = probe.palmmnt;
//                        verifyFeature.feature2 = gallery.palmmnt;
//                        HSFPFourPalm.VerifyFeature.Result result = new HSFPFourPalm.VerifyFeature.Result();
//                        try {
//                            result = HbieUtil.getInstance().hbie_PP.process(verifyFeature);
//                        } catch (RemoteException e) {
//                            e.printStackTrace();
//                        } catch (MatcherException e) {
//                            e.printStackTrace();
//                        }
//                        ppttRec.score = result.score;
                        list.add(ppttRec);
                    }
                }
            }

            list = CommonUtil.sort(list);
            for (int i = 0; i < list.size(); i++) {
                list.get(i).candrank = i + 1;
            }
            boolean isSuc = ppttdao.updateRes(list);
            if (isSuc) {
                srchTaskBean.setSTATUS(5);
                log.info("1ToF_PPTT search finished. ProbeId={}", srchTaskBean.getPROBEID());
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), 5, null);
            } else {
                exptMsg.append(PPTT_tablename).append(" Insert error").append(srchTaskBean.getTASKIDD());
                log.error("1ToF_PPTT search results insert into {} error. ProbeId={}", PPTT_tablename, srchTaskBean.getPROBEID());
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

    public String getPPTT_tablename() {
        return PPTT_tablename;
    }

    public void setPPTT_tablename(String PPTT_tablename) {
        this.PPTT_tablename = PPTT_tablename;
    }
}
