package HAFPIS.service;

import HAFPIS.DAO.SrchTaskDAO;
import HAFPIS.Utils.CommonUtil;
import HAFPIS.domain.SrchTaskBean;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 描述：
 * 作者：ZP
 * 创建时间:2017/8/22
 * 最后修改时间:2017/8/22
 */
public abstract class Recog {
    public int type;
    public String interval;
    public String queryNum;
    public String status;
    public String tablename;
    public int[] tasktypes = new int[2];
    public int[] datatypes = new int[2];
    public SrchTaskDAO srchTaskDAO;
    public ExecutorService executorService = Executors.newFixedThreadPool(3);
    public CommonUtil.BoundedExecutor boundedExecutor = new CommonUtil.BoundedExecutor(executorService, 6);
    public ArrayBlockingQueue<SrchTaskBean> srchTaskBeanArrayBlockingQueue = new ArrayBlockingQueue<>(20);

}
