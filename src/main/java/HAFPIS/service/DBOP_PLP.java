package HAFPIS.service;

import HAFPIS.DAO.DbopTaskDAO;
import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.DbopTaskBean;
import com.hisign.bie.MatcherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * DBOP-TPP
 * Created by ZP on 2017/5/19.
 */
public class DBOP_PLP extends Recog implements Runnable {
    private final Logger log = LoggerFactory.getLogger(DBOP_PLP.class);

    int datatype = 0;
    private DbopTaskDAO dbopTaskDAO;

    @Override
    public void run() {
        StringBuilder exptmsg = new StringBuilder();
        dbopTaskDAO = new DbopTaskDAO(tablename);
        if (CONSTANTS.DBOP_PLP == type) {
            datatype = 5;
        } else {
            log.error("wrong datatype {}, the thread will stop", type);
            System.exit(-1);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("----------------");
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            executorService.shutdown();
            dbopTaskDAO.updateStatus(5);
            System.out.println("DBOP_PLP executorservice is shutting down");
        }));
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
                    log.debug("sleeping");
                } catch (InterruptedException e) {
                    log.warn("Waiting Thread was interrupted: {}", e);
                }
            }else {
//                DbopTaskBean dbopTaskBean = null;
                List<Future<String>> listF = new ArrayList<>();
                for (final DbopTaskBean dbopTaskBean : list) {
                    dbopTaskBean.setStatus(4);
                    dbopTaskDAO.update(dbopTaskBean.getTaskIdd(), 4, null);
                    Future<String> f = executorService.submit(() -> {
                        try {
                            int tasktype = dbopTaskBean.getTaskType();
                            String id = dbopTaskBean.getProbeId();
                            switch (tasktype) {
                                case 6:
                                    if (HbieUtil.getInstance().hbie_PLP != null) {
                                        HbieUtil.getInstance().hbie_PLP.updateMatcher(id, -1);
                                    }
                                    break;
                                case 5:
                                    if (HbieUtil.getInstance().hbie_PLP != null) {
                                        HbieUtil.getInstance().hbie_PLP.updateMatcher(id, 1);
                                    }
                                    break;
                                case 7:
                                    if (HbieUtil.getInstance().hbie_PLP != null) {
                                        HbieUtil.getInstance().hbie_PLP.updateMatcher(id, -1);
                                        HbieUtil.getInstance().hbie_PLP.updateMatcher(id, 1);
                                    }
                                    break;
                                default:
                                    log.error("tasktype error {}.", tasktype);
                                    break;
                            }
                        } catch (RemoteException | MatcherException e) {
                            log.warn("matcher error: ", e);
                            dbopTaskDAO.update(dbopTaskBean.getTaskIdd(), 3, "matcher error " + e);
                        }
                    }, dbopTaskBean.getTaskIdd());
                    listF.add(f);
                }
                String taskid = null;
                for (Future<String> temp : listF) {
                    while (true) {
                        try {
                            taskid = temp.get();
                        } catch (InterruptedException | ExecutionException e) {
                            log.error("get future result error. ", e);
                        }
                        if (taskid != null) {
                            boolean is = dbopTaskDAO.update(taskid, 5, null);
                            if (is) {
                                log.info("DbopTask taskid-{} finish.", taskid);
                            } else {
                                log.info("DbopTask taskid-{} update error.", taskid);
                                dbopTaskDAO.update(taskid, -1, "update "+taskid+" error");
                            }
                            break;
                        }
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
}
