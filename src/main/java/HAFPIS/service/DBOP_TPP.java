package HAFPIS.service;

import HAFPIS.DAO.DbopTaskDAO;
import HAFPIS.DAO.PINFODAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.DbopTaskBean;
import com.hisign.bie.MatcherException;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * DBOP-TPP
 * Created by ZP on 2017/5/19.
 */
public class DBOP_TPP implements Runnable {
    private final Logger log = LoggerFactory.getLogger(DBOP_TPP.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private int type;
    private String interval;
    private String queryNum;
    private String status;
    private String tablename;
    private String tablename_pinfo;
    int datatype = 0;
    private DbopTaskDAO dbopTaskDAO;
    private PINFODAO pinfodao;

    @Override
    public void run() {
        if (CONSTANTS.DBOP_TPP == type) {
            datatype = 3;
        } else {
            log.error("wrong datatype {}, the thread will stop", type);
            System.exit(-1);
        }
        dbopTaskDAO = new DbopTaskDAO(tablename);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        while (true) {
            List<DbopTaskBean> list = new ArrayList<>();
            list = dbopTaskDAO.get(status, datatype, queryNum);
            if (null == list || list.size() == 0) {
                int timeSleep = 1;
                try {
                    timeSleep = Integer.parseInt(interval);
                } catch (NumberFormatException e) {
                    log.error("interval {} format error. Use default interval(1)", interval);
                }
                try {
                    timeSleep = Integer.parseInt(interval);
                } catch (NumberFormatException e) {
                    log.error("interval format error. should be number {}", interval);
                    timeSleep = 1;
                }
                try {
                    Thread.sleep(timeSleep * 1000);
                    log.info("sleeping");
                } catch (InterruptedException e) {
                    log.warn("Waiting Thread was interrupted: {}", e);
                }
            }else {
//                DbopTaskBean dbopTaskBean = null;
                List<Future<String>> listF = new ArrayList<>();
                for (int i = 0; i < list.size(); i++) {
                    final DbopTaskBean dbopTaskBean = list.get(i);
                    dbopTaskBean.setStatus(4);
                    dbopTaskDAO.update(dbopTaskBean.getTaskIdd(), 4, null);
                    Future<String> f = executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                int tasktype = dbopTaskBean.getTaskType();
                                String id = dbopTaskBean.getProbeId();
                                switch (tasktype) {
                                    case 6:
                                        HbieUtil.getInstance().hbie_FP.updateMatcher(id, -1);
                                        HbieUtil.getInstance().hbie_FP.updateMatcher(id + "_", -1);
                                        HbieUtil.getInstance().hbie_PP.updateMatcher(id, -1);
                                        HbieUtil.getInstance().hbie_FACE.updateMatcher(id, -1);
                                        HbieUtil.getInstance().hbie_IRIS.updateMatcher(id, -1);
                                        break;
                                    case 5:
                                        pinfodao = new PINFODAO(tablename_pinfo);
                                        String imgmask = pinfodao.getImgMask(id);
                                        if (imgmask == null) {
                                            log.error("can not get imgmask for probeid: {}", id);
                                            imgmask = "11111111111111111111111111111111111";
                                        }
                                        if (imgmask.length() >= 10) {
                                            String rfp = imgmask.substring(0, 10);
                                            if (!"0000000000".equals(rfp)) {
//                                                HbieUtil.hbie_FP.updateMatcher(id, 1);
                                                HbieUtil.getInstance().hbie_FP.updateMatcher(id, 1);
                                            }
                                        }
                                        if (imgmask.length() >= 20) {
                                            String ffp = imgmask.substring(10, 20);
                                            if (!"0000000000".equals(ffp)) {
//                                                HbieUtil.hbie_FP.updateMatcher(id + "_", 1);
                                                HbieUtil.getInstance().hbie_FP.updateMatcher(id + "_", 1);
                                            }
                                        }
                                        if (imgmask.length() >= 30) {
                                            String pm = imgmask.substring(20, 30);
                                            if (!"0000000000".equals(pm)) {
//                                                HbieUtil.hbie_PP.updateMatcher(id, 1);
                                                HbieUtil.getInstance().hbie_PP.updateMatcher(id, 1);
                                            }
                                        }
                                        if (imgmask.length() >= 31) {
                                            String face = imgmask.substring(30, 31);
                                            if (face.charAt(0) == '1') {
//                                                HbieUtil.hbie_FACE.updateMatcher(id, 1);
                                                HbieUtil.getInstance().hbie_FACE.updateMatcher(id, 1);
                                            }
                                        }
                                        if (imgmask.length() >= 35) {
                                            String iris = imgmask.substring(33, 35);
                                            if (!"00".equals(iris)) {
//                                                HbieUtil.hbie_IRIS.updateMatcher(id, 1);
                                                HbieUtil.getInstance().hbie_IRIS.updateMatcher(id, 1);

                                            }
                                        }
                                        break;
                                    case 7:
                                        HbieUtil.getInstance().hbie_FP.updateMatcher(id, -1);
                                        HbieUtil.getInstance().hbie_FP.updateMatcher(id, 1);
                                        HbieUtil.getInstance().hbie_FP.updateMatcher(id + "_", -1);
                                        HbieUtil.getInstance().hbie_FP.updateMatcher(id + "_", 1);
                                        HbieUtil.getInstance().hbie_PP.updateMatcher(id, -1);
                                        HbieUtil.getInstance().hbie_PP.updateMatcher(id, 1);
                                        HbieUtil.getInstance().hbie_FACE.updateMatcher(id, -1);
                                        HbieUtil.getInstance().hbie_FACE.updateMatcher(id, 1);
                                        HbieUtil.getInstance().hbie_IRIS.updateMatcher(id, -1);
                                        HbieUtil.getInstance().hbie_IRIS.updateMatcher(id, 1);
                                        break;
                                    default:
                                        log.error("tasktype error {}.", tasktype);
                                        break;
                                }
                            } catch (RemoteException | MatcherException e) {
                                log.warn("matcher error: ", e);
                                dbopTaskDAO.update(dbopTaskBean.getTaskIdd(), 3, "matcher error " + e);
                            }
                        }
                    }, dbopTaskBean.getTaskIdd());
                    listF.add(f);
                }
                String taskid = null;
                for (int i = 0; i < listF.size(); i++) {
                    Future<String> temp = listF.get(i);
                    try {
                        taskid = temp.get();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("get future result error. ", e);
                    }
                }
                if (taskid != null) {
                    boolean is = dbopTaskDAO.update(taskid, 5, null);
                    if (is) {
                        log.info("DbopTask taskid-{} finish.", taskid);
                    } else {
                        log.warn("DbopTask taskid-{} update table error.", taskid);
                        dbopTaskDAO.update(taskid, 5, null);
                    }
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

    public String getTablename_pinfo() {
        return tablename_pinfo;
    }

    public void setTablename_pinfo(String tablename_pinfo) {
        this.tablename_pinfo = tablename_pinfo;
    }
}
