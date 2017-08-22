package HAFPIS.DAO;

import HAFPIS.Utils.DateUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.DbopTaskBean;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * HAFPIS_DBOP_TASK
 * Created by ZP on 2017/5/19.
 */
public class DbopTaskDAO {
    private final Logger log = LoggerFactory.getLogger(SrchTaskDAO.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private String tablename = null;

    public DbopTaskDAO(String tablename) {
        this.tablename = tablename;
    }


    public synchronized List<DbopTaskBean> get(String status, int datatype, String queryNum) {
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tablename);
        sb.append(" where status=").append(status);
        sb.append(" and datatype=").append(datatype);
        sb.append(" and rownum<=").append(queryNum);
        sb.append(" order by priority desc, begtime asc");
        List<DbopTaskBean> list = new ArrayList<>();
        try {
            list = qr.query(sb.toString(), new BeanListHandler<DbopTaskBean>(DbopTaskBean.class));
        } catch (SQLException e) {
            log.error("get DBOPTask error. SQLException: {}, query_sql: {}", e, sb.toString());
        }
        return list;
    }

    public synchronized boolean update(String taskidd, int status, String exptmsg) {
        List<Object> param = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(tablename);
        sb.append(" set status=?,").append(" endtime=?");
        param.add(status);
        String date = DateUtil.getFormatDate(System.currentTimeMillis());
        param.add(date);
        if (status < 0 || exptmsg != null) {
            sb.append(", exptmsg=?");
            if (exptmsg.length() > 128) {
                exptmsg = exptmsg.substring(1, 128);
            }
            param.add(exptmsg);
        }
        sb.append(" where taskidd=?");
        param.add(taskidd);
        try {
            return qr.update(sb.toString(), param.toArray()) > 0;
        } catch (SQLException e) {
            log.error("update DBOPTASK error. ", e);
            return false;
        }
    }

    public synchronized void updateStatus(int datatype) {
        StringBuilder sql = new StringBuilder("update ");
        sql.append(tablename).append(" set status=3 where status=4 and datatype=?");
        try{
            qr.update(sql.toString(), datatype);
        } catch (SQLException e) {
            log.error("update status error before shutting down");
        }
    }
}
