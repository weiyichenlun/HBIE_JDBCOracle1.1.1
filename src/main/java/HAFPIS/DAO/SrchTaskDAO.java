package HAFPIS.DAO;

import HAFPIS.Utils.DateUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.SrchTaskBean;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * HAFPIS_SRCH_TASK
 * Created by ZP on 2017/5/15.
 */
public class SrchTaskDAO {
    private final Logger log = LoggerFactory.getLogger(SrchTaskDAO.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private String tablename = null;

    public SrchTaskDAO(String tablename) {
        this.tablename = tablename;
    }


    public synchronized List<SrchTaskBean> getList(String status, int[] datatypes, int[] tasktypes, String queryNum) {
        List<SrchTaskBean> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tablename);
        int statusN = 0;
        try {
            statusN = Integer.parseInt(status);
        } catch (NumberFormatException e) {
            log.error("parse status error. status must be a number. status-{}, exception-{}", status, e);
        }
        sb.append(" where status=").append(statusN);
        sb.append(" and datatype in (");
        for (int i=0; i<datatypes.length; i++) {
            if (datatypes[i] > 0) {
                sb.append(datatypes[i]).append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1).append(")");
        sb.append(" and tasktype in (");
        for (int tasktype : tasktypes) {
            if (tasktype != 0) {
                sb.append(tasktype).append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1).append(")");
        sb.append(" and rownum<=").append(Integer.parseInt(queryNum));
        sb.append(" order by priority desc, endtime asc");
        try {
            list = qr.query(sb.toString(), new BeanListHandler<SrchTaskBean>(SrchTaskBean.class));
            log.debug("query_sql is {}", sb.toString());
        } catch (SQLException e) {
            log.error("SQLException: {}, query_sql:{}", e, sb.toString());
        }
        return list;
    }


    public synchronized void update(String taskidd, int status, String exptmsg) {
        StringBuilder sb = new StringBuilder();
        List<Object> param = new ArrayList<>();
        sb.append("update ").append(tablename).append(" set");
        sb.append(" status=?,");
        param.add(status);
        sb.append(" endtime=?");
        String date = DateUtil.getFormatDate(System.currentTimeMillis());
        param.add(DateUtil.getFormatDate(System.currentTimeMillis()));
        if (status < 0 || exptmsg != null) {
            sb.append(", exptmsg=?");
            if (exptmsg.length() > 127) {
                exptmsg = exptmsg.substring(0, 127);
            }
            param.add(exptmsg);
        }
        sb.append(" where taskidd=?");
        param.add(taskidd);
        try {
            qr.update(sb.toString(), param.toArray());
        } catch (SQLException e) {
            log.error("UPDATE DB error: ", e);
        }
    }
}
