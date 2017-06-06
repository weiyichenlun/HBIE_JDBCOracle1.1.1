package HAFPIS.service;

import HAFPIS.DAO.PPTTDAO;
import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.PPTTRec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import com.hisign.bie.MatcherException;
import com.hisign.bie.hsfp.HSFPFourPalm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by ZP on 2017/5/19.
 */
public class OneToF_PPTT implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(OneToF_PPTT.class);
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private String PPTT_tablename;
    private int[] tasktypes = new int[2];
    private int[] datatypes = new int[2];
    private SrchTaskDAO srchTaskDAO;

    @Override
    public void run() {
        srchTaskDAO = new SrchTaskDAO(tablename);
        if (type == CONSTANTS.PPTT1TOF) {
            tasktypes[0] = 8;
            datatypes[0] = 2;
        } else {
            log.warn("PPTT_1ToF the type is wrong. type={}", type);
        }
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
        ExecutorService executorService = Executors.newFixedThreadPool(5);
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
//                        for (int j = 0; j < 4; j++) {
//                            feature1[i] = probe.palmmnt[i];
//                            feature2[i] = gallery.palmmnt[i];
//                            Future<Float> score = executorService.submit(new Callable<Float>() {
//                                @Override
//                                public Float call() throws Exception {
//                                    //TODO remain to be implemented
//                                    HSFPFourPalm.VerifyFeature verifyFeature = new HSFPFourPalm.VerifyFeature();
//                                    verifyFeature.feature1 = feature1;
//                                    verifyFeature.feature2 = feature2;
//                                    HSFPFourPalm.VerifyFeature.Result result = HbieUtil.hbie_PP.process(verifyFeature);
//                                    return result.score;
//                                }
//                            });
//                            map.put(j, score);
//                        }
//                        float tempScore = 0F;
//                        for (int j = 0; j < map.size(); j++) {
//                            Future<Float> f = map.get(j);
//                            float temp = 0F;
//                            try {
//                                temp = f.get();
//                                System.out.println("j=" + j + "scores is " + temp);
//                            } catch (InterruptedException | ExecutionException e) {
//                                log.info("get 1ToF score map error, probeid={} ", new String(gallery.probeId), e);
//
//                            }
//                            if (temp > tempScore) {
//                                tempScore = temp;
//                            }
//                            ppttRec.ppscores[j] = temp;
//                        }
                        HSFPFourPalm.VerifyFeature verifyFeature = new HSFPFourPalm.VerifyFeature();
                        verifyFeature.feature1 = probe.palmmnt;
                        log.info("probe.palmmnt:" + probe.palmmnt.length);
                        for (int j = 0; j < 4; j++) {
                            byte[] fea = probe.palmmnt[j];
                            for (int k = 0; k < fea.length; k++) {
                                System.out.print(fea[k]+" ");
                            }
                            System.out.println();
                        }
                        verifyFeature.feature2 = gallery.palmmnt;
                        log.info("gallery.palmmnt:" + gallery.palmmnt.length);
                        HSFPFourPalm.VerifyFeature.Result result = new HSFPFourPalm.VerifyFeature.Result();
                        try {
                            result = HbieUtil.getInstance().hbie_PP.process(verifyFeature);
                        } catch (RemoteException e) {
                            e.printStackTrace();
                        } catch (MatcherException e) {
                            e.printStackTrace();
                        }
                        ppttRec.score = result.score;
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
                srchTaskDAO.update(srchTaskBean.getTASKIDD(), -1, exptMsg.toString().substring(1, 128));
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
