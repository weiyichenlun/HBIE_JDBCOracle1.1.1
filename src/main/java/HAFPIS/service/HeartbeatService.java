package HAFPIS.service;

import HAFPIS.DAO.HeartBeatDAO;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.HeartBeatBean;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 描述：
 * 作者：ZP
 * 创建时间:2018/3/12
 * 最后修改时间:2018/3/12
 */
public class HeartbeatService extends Recog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private HeartBeatDAO heartBeatDAO;
    private String heartbeat_table = null;
    private String heartbeat_instance_name = null; //插入到心跳表中的实例名
    private int instance_name_rank = 0; //插入到心跳表中的实例名启动标志 0:未启动 1:已启动
    private int heartbeat_interval = 5; //心跳检测间隔 默认5s
    private int heartbeat_retry = 20; //实例切换间隔 默认20s
    private long lastUpdateTime = 0L;
    private int retry_time = 0;
    private long start_time = 1;

    @Override
    public void run() {
//        instance_name_rank = Integer.parseInt(heartbeat_instance_name.substring(heartbeat_instance_name.length() - 1));

        if (heartbeat_table == null) {
            log.error("没有指定心跳表，无法启动程序(No heartbeat table config)");
            System.exit(-1);
        } else {
            heartBeatDAO = new HeartBeatDAO(this.heartbeat_table);

            //TODO update time first





            //TODO check status 检测由于异常关闭造成的相关记录没有清零的情况
            checkStatus(false);
            heartBeatDAO.delete(heartbeat_instance_name);
//            long updatetime = System.currentTimeMillis();
            long updatetime = start_time;
            log.info("insert record id/start/time: {}/{}/{}", heartbeat_instance_name, 1, updatetime);
            while (true) {
                try {
                    if (heartBeatDAO.insert(heartbeat_instance_name, 1, updatetime)) break;
                    else continue;
                } catch (Exception e) {
                    log.error("database error. ", e);
                    CommonUtil.sleep("10");
                }
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("---------------");
                heartBeatDAO.updateRankno(0, heartbeat_instance_name);
                log.info("Heartbeat service is shutting down");
            }));

            try {
                //try twice
                CommonUtil.sleep("" + heartbeat_interval);
                HeartBeatBean heartBeatBean = heartBeatDAO.queryLatest();
                String dead_name = heartBeatBean.getINSTANCENAME();
                lastUpdateTime = heartBeatBean.getUPDATETIME();
                CommonUtil.sleep("" + heartbeat_interval);
                HeartBeatBean heartBeatBean1 = heartBeatDAO.queryLatest();
                if (!heartBeatBean1.getINSTANCENAME().equals(dead_name)) {
                    dead_name = heartBeatBean1.getINSTANCENAME();
                    lastUpdateTime = heartBeatBean1.getUPDATETIME();
                    heartBeatBean = heartBeatBean1;
                }
                while (true) {
                    if (heartBeatBean.getINSTANCENAME().equals(heartbeat_instance_name) && lastUpdateTime > 0) {
                        log.info("Current active instance is {}, this instance is {}", heartBeatBean.getINSTANCENAME(), heartbeat_instance_name);
                        break;
                    } else if (!heartBeatBean.getINSTANCENAME().equals(heartbeat_instance_name)) {
                        log.info("Current active instance: {}, but this instance is {}", heartBeatBean.getINSTANCENAME(), heartbeat_instance_name);
                        Thread.sleep(heartbeat_interval * 1000);
                        heartBeatBean = heartBeatDAO.queryLatest();
                        if (heartBeatBean.getUPDATETIME() == lastUpdateTime) {
                            retry_time++;
                        } else {
                            lastUpdateTime = heartBeatBean.getUPDATETIME();
                            retry_time = 0;
                        }
                        if (retry_time >= heartbeat_retry) {
                            log.info("instance is not active anymore {}*{} s past", heartbeat_interval, heartbeat_retry);
                            checkStatus(true);
                            List<HeartBeatBean> beanList = heartBeatDAO.querySome(1);
                            if (beanList != null && beanList.get(0).getINSTANCENAME().equals(heartbeat_instance_name)) {
                                heartBeatDAO.updateRankno(0, heartBeatBean.getINSTANCENAME());
                                break;
                            }
                        }

                    } /*else if (heartBeatBean.getUPDATETIME() == 0) {
                        Thread.sleep(heartbeat_interval * 1000);
                        heartBeatBean = heartBeatDAO.queryLatest();
                        if(heartBeatBean.getUPDATETIME() == lastUpdateTime) retry_time ++;
                        if (retry_time > heartbeat_retry) {
                            log.info("Current instance is not active anymore {}*{} s past", heartbeat_interval, heartbeat_retry);
                        }
                        List<HeartBeatBean> beanList = heartBeatDAO.querySome(2);
                        if (beanList != null && beanList.get(1).getINSTANCENAME().equals(heartbeat_instance_name)) {
                            heartBeatDAO.update(0, dead_name);
                        }
                    }*/
                }
            } catch (InterruptedException e) {
                log.error("Error. ", e);
            }

            new Thread(() -> {
                    while (true) {
                        try {
                            start_time += heartbeat_interval;
                            heartBeatDAO.update(start_time, heartbeat_instance_name);
//                            heartBeatDAO.update(System.currentTimeMillis(), heartbeat_instance_name);
                            Thread.sleep(heartbeat_interval * 1000);
                        } catch (InterruptedException e) {
                            log.error("Impossible. ", e);
                        }
                    }
            }, "Heartbeat_Thread").start();
        }
    }

    public void checkStatus(boolean dead) {
        List<HeartBeatBean> list = heartBeatDAO.queryAll();
        if (list != null && list.size() > 0) {
            Map<String, Long> res = new HashMap<>();
            for (int i = 0; i < list.size(); i++) {
                HeartBeatBean bean = list.get(i);
                res.put(bean.getINSTANCENAME(), bean.getUPDATETIME());
            }
            CommonUtil.sleep("" + (heartbeat_interval + 1));
            List<HeartBeatBean> list1 = heartBeatDAO.queryAll();
            if (list1 != null && list.size() > 0) {
                for (int i = 0; i < list1.size(); i++) {
                    HeartBeatBean bean1 = list1.get(i);
                    if (res.get(bean1.getINSTANCENAME()) == bean1.getUPDATETIME()) {
                        if (dead) {
                            heartBeatDAO.updateRankno(0, bean1.getINSTANCENAME());
                        } else {
                            if (bean1.getRANKNO() == 1 && bean1.getUPDATETIME() == 1) {
                                log.info("instance: {} is waitiing...", bean1.getINSTANCENAME());
                            } else {
                                heartBeatDAO.updateRankno(1, bean1.getINSTANCENAME());
                            }
                        }
                    }
                }
            }
        }
    }

    public String getHeartbeat_table() {
        return heartbeat_table;
    }

    public void setHeartbeat_table(String heartbeat_table) {
        this.heartbeat_table = heartbeat_table;
    }

    public String getHeartbeat_instance_name() {
        return heartbeat_instance_name;
    }

    public void setHeartbeat_instance_name(String heartbeat_instance_name) {
        this.heartbeat_instance_name = heartbeat_instance_name;
    }

    public int getHeartbeat_interval() {
        return heartbeat_interval;
    }

    public void setHeartbeat_interval(int heartbeat_interval) {
        this.heartbeat_interval = heartbeat_interval;
    }

    public int getHeartbeat_retry() {
        return heartbeat_retry;
    }

    public void setHeartbeat_retry(int heartbeat_retry) {
        this.heartbeat_retry = heartbeat_retry;
    }
}
