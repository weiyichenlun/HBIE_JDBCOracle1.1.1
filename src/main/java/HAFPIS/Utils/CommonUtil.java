package HAFPIS.Utils;

import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.domain.Rec;
import HAFPIS.domain.SrchDataRec;
import HAFPIS.domain.SrchTaskBean;
import HAFPIS.service.Recog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 通用工具类
 * Created by ZP on 2017/5/15.
 */
public class CommonUtil {
    private static final Logger log = LoggerFactory.getLogger(CommonUtil.class);

    /**
     * Blob转Record
     * @param srchdata Blob数据
     * @param datatype 数据类型代码
     * @return Record数组
     */
    public static synchronized List<SrchDataRec> srchdata2Rec(Blob srchdata, int datatype) {
        List<SrchDataRec> result = new ArrayList<SrchDataRec>();

        try {
            InputStream is = srchdata.getBinaryStream();
            DataInputStream dis = new DataInputStream(is);

            while (true) {
                SrchDataRec temp = new SrchDataRec();
                temp.datatype = datatype;
                // probeid
                for (int j = 0; j < temp.probeId.length; j++) {
                    temp.probeId[j] = dis.readByte();
                }
                // RPMNT RPImg
                for (int j = 0; j < temp.RpMntLen.length; j++) {
                    temp.RpMntLen[j] = dis.readInt();
                }
                for (int j = 0; j < temp.RpImgLen.length; j++) {
                    temp.RpImgLen[j] = dis.readInt();
                }
                // FPMNT flatImg
                for (int j = 0; j < temp.FpMntLen.length; j++) {
                    temp.FpMntLen[j] = dis.readInt();
                }
                for (int j = 0; j < temp.FpImgLen.length; j++) {
                    temp.FpImgLen[j] = dis.readInt();
                }
                // palmMnt palmImg
                for (int j = 0; j < temp.PalmMntLen.length; j++) {
                    temp.PalmMntLen[j] = dis.readInt();
                }
                for (int j = 0; j < temp.PalmImgLen.length; j++) {
                    temp.PalmImgLen[j] = dis.readInt();
                }
                // faceMnt faceImg
                for (int j = 0; j < temp.FaceMntLen.length; j++) {
                    temp.FaceMntLen[j] = dis.readInt();
                }
                for (int j = 0; j < temp.FaceImgLen.length; j++) {
                    temp.FaceImgLen[j] = dis.readInt();
                }
                // irisMnt irisImg
                for (int j = 0; j < temp.IrisMntLen.length; j++) {
                    temp.IrisMntLen[j] = dis.readInt();
                }
                for (int j = 0; j < temp.IrisImgLen.length; j++) {
                    temp.IrisImgLen[j] = dis.readInt();
                }
                // reservered
                for (int j = 0; j < temp.reserved.length; j++) {
                    temp.reserved[j] = dis.readInt();
                }
                switch (datatype) {
                    case 1:
                        for (int i = 0; i < temp.RpMntLen.length; i++) {
                            if (temp.RpMntLen[i] == 0) {
                                temp.rpmnt[i] = null;
                            } else {
                                byte[] tempFea = new byte[temp.RpMntLen[i]];
                                dis.readFully(tempFea);
                                temp.rpmnt[i] = tempFea;
                                temp.rpmntnum++;
                            }
                        }
                        for (int i = 0; i < temp.FpMntLen.length; i++) {
                            if (temp.FpMntLen[i] == 0) {
                                temp.fpmnt[i] = null;
                            } else {
                                byte[] tempFea = new byte[temp.FpMntLen[i]];
                                dis.readFully(tempFea);
                                temp.fpmnt[i] = tempFea;
                                temp.fpmntnum++;
                            }
                        }
                        break;
                    case 4:
                        if (temp.RpMntLen[0] == 0) {
                            temp.latfpmnt = null;
                        } else {
                            int len = temp.RpMntLen[0];
                            if (len == 6304) {
                                byte[] head = new byte[160];
                                dis.readFully(head);
                                byte[] tempFea1 = new byte[3072];
                                byte[] tempFea2 = new byte[3072];
                                dis.readFully(tempFea1);
                                dis.readFully(tempFea2);
                                temp.latfpmnt = tempFea1;
                                temp.latfpmnt_auto = tempFea2;
                            } else if (len == 3072) {
                                byte[] tempFea1 = new byte[3072];
                                dis.readFully(tempFea1);
                                temp.latfpmnt = tempFea1;
                                temp.latfpmnt_auto = null;
                            }
                        }

                        break;
                    case 2:
                        for (int i = 0; i < 4; i++) {
                            int len = temp.PalmMntLen[CONSTANTS.srchOrder[i]];
                            if (len == 0) {
                                temp.palmmnt[CONSTANTS.feaOrder[i]] = null;
                            } else {
                                byte[] tempFea = new byte[len];
                                dis.readFully(tempFea);
                                temp.palmmnt[CONSTANTS.feaOrder[i]] = tempFea;
                                temp.palmmntnum++;
                            }
                        }
                        break;
                    case 5:
                        if (temp.PalmMntLen[0] == 0) {
                            temp.latpalmmnt = null;
                        } else {
                            byte[] tempFea = new byte[temp.PalmMntLen[0]];
                            dis.readFully(tempFea);
                            temp.latpalmmnt = tempFea;
                        }
                        break;
                    case 6:
                        for (int i = 0; i < 3; i++) {
                            int len = temp.FaceMntLen[i];
                            if (len == 0) {
                                temp.facemnt[i] = null;
                            } else {
                                byte[] tempFea = new byte[len];
                                dis.readFully(tempFea);
                                temp.facemnt[i] = tempFea;
                                temp.facemntnum++;
                            }
                        }
                        break;
                    case 7:
                        for (int i = 0; i < 2; i++) {
                            int len = temp.IrisMntLen[i];
                            if (len == 0) {
                                temp.irismnt[i] = null;
                            } else {
                                byte[] tempFea = new byte[len];
                                dis.readFully(tempFea);
                                temp.irismnt[i] = tempFea;
                                temp.irismntnum++;
                            }
                        }
                        break;
                }
                result.add(temp);
            }
        } catch (SQLException e) {
            log.error("get srchdata binarystream error. ", e);
            return null;
        } catch (IOException e) {
            if (e instanceof EOFException) {
                log.info("convert srchdata finished!");
                return result;
            } else {
                log.error("deal with srch data error. ", e);
                return null;
            }
        }

    }


    public synchronized static <T extends Rec> List<T> mergeResult(List<T> list) {
        if (list == null || list.size() == 0) {
            return null;
        }
        Rec fpRec1, fpRec2;
        for (int i = 0; i < list.size() - 1; i++) {
            for (int j = list.size() - 1; j > i; j--) {
                fpRec1 = list.get(i);
                fpRec2 = list.get(j);
                if (fpRec1.position == fpRec2.position) {
                    float tempScore = Math.max(fpRec1.score, fpRec2.score);
                    if (fpRec2.candid.endsWith("_") && fpRec1.candid.equals(fpRec2.candid.substring(0, fpRec2.candid.length() - 1))) {
                        fpRec1.score = tempScore;
                        list.remove(j);
                    }
                    if (fpRec1.candid.endsWith("_") && fpRec2.candid.equals(fpRec1.candid.substring(0, fpRec1.candid.length() - 1))) {
                        fpRec1.score = tempScore;
                        list.remove(j);
                    }
                    if (fpRec1.candid.equals(fpRec2.candid)) {
                        fpRec1.score = tempScore;
                        list.remove(j);
                    }
                }
            }
        }
        for (T aList : list) {
            if (aList.candid.endsWith("_")) {
                aList.candid = aList.candid.substring(0, aList.candid.length() - 1);
            }
        }
        return sort(list);
    }


    public static <T extends Comparable<? super T>> List<T> sort(List<T> list){
        Collections.sort(list);
        return list;
    }

    public static <T extends Rec> List<T> getList(List<T> list, int numOfCand){
        List<T> res = new ArrayList<>();
        if (list.size() > numOfCand) {
            res.addAll(list.subList(0, numOfCand));
        } else {
            res.addAll(list);
        }
        return res;
    }

    public synchronized static <T extends Rec> List<T> mergeResult(List<T> list, List<T> list_rest) {
        List<T> res = new ArrayList<>();
        if(list == null && list_rest == null){
            return null;
        }
        else if(list == null){
            res.addAll(list_rest);
        }
        else if(list_rest == null){
            res.addAll(list);
        }
        else{
            res.addAll(list);
            res.addAll(list_rest);
        }
        return mergeResult(res);
    }

    public synchronized static String getFilter(Clob clob) {
        String filter = null;
        if (clob != null) {
            try {
                int len = (int) clob.length();
                if (len <= 0) {
                    return null;
                }
                char[] temp = new char[len];
                try {
                    int n = clob.getCharacterStream().read(temp, 0, len);
                } catch (IOException e) {
                    log.error("clob get stream error");
                }
//                filter = clob.getSubString(1, len);
                filter = new String(temp);
                filter = decode(filter);
                log.info("The demofilter is: \n"+filter);
            } catch (SQLException e) {
                log.error("get filter from clob error. ", e);
            }
//            return "(" + filter + ")";
            return filter;
        }
        return null;
    }

    /**
     * 根据solveordup项获取相关的过滤项，用来控制重卡是否参与比对
     * @param type
     * @param solveordup
     * @return
     */
    public static String getSolveOrDupFilter(int type, Integer solveordup) {
        int num = solveordup == null ? 0 : solveordup;
        String res = null;
        if (type == CONSTANTS.DBOP_TPP) {
            if (num == 0) {
                res = "TPCARDDUP=={" + num + "}";
            } else if (num == 1) {
//                res = "TPCARDDUP=={0}||TPCARDDUP=={" + num + "}";
                res = null;
            }
        } else if (type == CONSTANTS.DBOP_LPP || type == CONSTANTS.DBOP_PLP) {
            if (num == 0) {
                res = "SOLVEATTR=={" + num + "}";
            } else if (num == 1) {
//                res = "SOLVEATTR=={0}||SOLVEATTR=={" + num + "}";
                res = null;
            }
        }
        return res;
    }

    public static String getDBsFilter(String srchDbMask) {
        StringBuilder filter = new StringBuilder();
        if (null == srchDbMask || srchDbMask.trim().isEmpty()) {
            return null;
        }
        for (int i = 0; i < srchDbMask.length(); i++) {
            if (srchDbMask.charAt(i) == '1') {
                filter.append("dbId=={").append(i+1).append("}").append("||");
            }
        }
        if (filter.length() >= 2) {
            filter.setLength(filter.length() - 2);
        }
        String filterStr = filter.toString();
        System.out.println("dbid filter is: "+filterStr);
        if (filterStr.trim().isEmpty()) {
            return null;
        }
        return filterStr;
//        return "(" + filterStr + ")";
    }

    /**
     * 解析字符串
     * @param s 16进制字符串
     * @return
     */
    public static String decode(String s) {
        byte[] res = new byte[s.length() / 2];
        for (int i = 0; i < res.length; i++) {
            try {
                res[i] = (byte) (0xff & Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16));
            } catch (NumberFormatException e) {
                log.error(e.toString());
            }

        }
        try {
            s = new String(res, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error(e.toString());
        }
        return s;
    }

    /**
     * 参数不定的合并过滤器，最后一项必须为DEMOFILTER过滤项
     * @param strings
     * @return 合并后的过滤条件
     */
    public static String mergeFilter(String... strings) {
        StringBuilder sb = new StringBuilder();
        boolean demoFilterEnable = ConfigUtil.getConfig("demo_filter_enable").equals("0");
        if (strings == null || strings.length == 0) {
            return null;
        }
        for (int i = 0; i < strings.length-1; i++) {
            if (strings[i] != null && strings[i].trim().length() > 0) {
                sb.append("(").append(strings[i]).append(")").append("&&");
            }
        }
        String demofilter = strings[strings.length - 1];
        if (!demoFilterEnable && demofilter != null && demofilter.trim().length() > 0) {
            sb.append("(").append(demofilter).append(")").append("&&");
        }
        if (sb.length() == 0) {
            return null;
        } else {
            sb.setLength(sb.length() - 2);
        }
        return sb.toString();
    }


    /**
     * check whether list is empty. If true, wait for interval seconds
     * @param list
     * @param interval
     */
    public static void checkList(List<SrchTaskBean> list, String interval) {
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
    }

    /**
     * 检查SrchTask中给定的SRCHPOSMASK的有效性
     * @param type 指纹或者掌纹比对类型
     * @param srchPosMask
     * @return 返回有效的SRCHPOSMASK
     */
    public static String checkSrchPosMask(int type, String srchPosMask) {
        if (type == CONSTANTS.FPTL || type == CONSTANTS.FPLT) {
            if (srchPosMask == null || srchPosMask.length() == 0) {
                srchPosMask = "11111111111111111111";
            } else if (srchPosMask.length() > 0 && srchPosMask.length() < 20) {
                char[] tempMask = "00000000000000000000".toCharArray();
                for (int i = 0; i < srchPosMask.length(); i++) {
                    if (srchPosMask.charAt(i) == '1') {
                        tempMask[i] = '1';
                    }
                }
                srchPosMask = String.valueOf(tempMask);
            } else {
                String temp = srchPosMask.substring(0, 20);
                if (temp.equals("00000000000000000000")) {
                    srchPosMask = "11111111111111111111";
                }
            }
            return srchPosMask;
        } else if (type == CONSTANTS.PPLT || type == CONSTANTS.PPTL) {
            if (srchPosMask == null || srchPosMask.length() == 0) {
                srchPosMask = "1000110001";
            } else if (srchPosMask.length() > 0 && srchPosMask.length() <= 10) {
                char[] tempMask = "0000000000".toCharArray();
                for (int i = 0; i < 4; i++) {
                    if (srchPosMask.charAt(CONSTANTS.srchOrder[i]) == '1') {
                        tempMask[CONSTANTS.srchOrder[i]] = '1';
                    }
                }
                srchPosMask = String.valueOf(tempMask);
            } else {
                srchPosMask = srchPosMask.substring(0, 10);
                if (srchPosMask.equals("0000000000")) {
                    srchPosMask = "1000110001";
                }
            }
            return srchPosMask;
        }
        return null;
    }

    /**
     * 有界缓存池，采用Semaphore来控制提交的任务数量，同时保证多线程
     */
    public static class BoundedExecutor {
        private final Executor exec;
        private final Semaphore semaphore;

        public BoundedExecutor(Executor exec, int bound) {
            this.exec = exec;
            this.semaphore = new Semaphore(bound);
        }

        public void submitTask(final Runnable command) throws InterruptedException, RejectedExecutionException {
            semaphore.acquire();
            try {
                exec.execute(()->{
                    try{
                        command.run();
                    } finally {
                        semaphore.release();
                    }
                });
            } catch (RejectedExecutionException e) {
                semaphore.release();
                throw e;
            }
        }

        public void close() {
            try{
                ((ExecutorService)exec).awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Interrupted during awaitTermination.", e);
            }
            ((ExecutorService) exec).shutdown();
        }
    }

    /**
     * 根据条件获取SrchTask
     * @param recog
     */
    public static void getList(Recog recog) {
//        List<SrchTaskBean> list = recog.srchTaskDAO.getList(recog.status, recog.datatypes, recog.tasktypes, recog.queryNum);
        int datatype=0, tasktype=0;
        List<SrchTaskBean> list = new ArrayList<>();
        for (int i = 0; i < recog.datatypes.length; i++) {
            datatype = recog.datatypes[i];
            tasktype = recog.tasktypes[i];
            if (datatype != 0 && tasktype != 0) {
                List<SrchTaskBean> temp = recog.srchTaskDAO.getList(recog.status, datatype, tasktype, recog.queryNum);
                if (temp.size() > 0) {
                    list.addAll(temp);
                }
            }
        }
        checkList(list, recog.interval);
        list.forEach(srchTaskBean -> {
            try{
                recog.srchTaskDAO.update(srchTaskBean.getTASKIDD(), 4, null);
                recog.srchTaskBeanArrayBlockingQueue.put(srchTaskBean);
            } catch (InterruptedException e) {
                log.warn("Error during put into srchTaskBean queue. taskidd is {}\n And will try again", srchTaskBean.getTASKIDD(), e);
            }
        });
    }

    public static <P> void getList(CallBack cb, Recog recog) {
        try {
            recog.boundedExecutor.submitTask(() -> cb.run(recog.srchTaskDAO));
        } catch (InterruptedException e) {
            log.error("Interrupted during CommonUtil.getList. ", e);
        }
    }

    public interface CallBack{
        void run(SrchTaskDAO srchTaskDAO);
    }


    public static void main(String[] args) {
    }
}
