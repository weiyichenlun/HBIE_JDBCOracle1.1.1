package HAFPIS.DAO;

import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.DateUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.DbopTaskBean;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
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


    public synchronized List<DbopTaskBean> get(String status, int datatype, String queryNum) throws SQLException {
        StringBuilder sb = new StringBuilder();

        if (ConfigUtil.getConfig("database").toLowerCase().equals("sqlserver")) {
//            sb.append("select * from (select top ").append(queryNum).append(" * from ");
//            sb.append(tablename);
//            sb.append(" where status=").append(status).append(" and datatype=").append(datatype);
//            sb.append(" order by priority desc, endtime asc) res");

            sb.append("select * from ").append(tablename).append(" where ROWID in (");
            sb.append("select top ").append(queryNum).append("RID from (select ROWID RID from ").append(tablename)
                    .append(" where status=").append(status).append(" and datatype=").append(datatype);
            sb.append(" order by priority desc, endtime asc)) for update");

        } else {
            sb.append("select * from ").append(tablename).append(" where ROWID in (");
            sb.append("select RID from ( select ROWID RID from ").append(tablename)
                    .append(" where status=").append(status).append(" and datatype=").append(datatype)
            .append(" order by priority desc, endtime asc)");
            sb.append(" where ROWNUM <=").append(queryNum).append(") for update");
        }

//        sb.append("select * from ").append(tablename);
//        sb.append(" where status=").append(status);
//        sb.append(" and datatype=").append(datatype);
//        sb.append(" and rownum<=").append(queryNum);
//        sb.append(" order by priority desc, begtime asc");
        List<DbopTaskBean> list = new ArrayList<>();
        try {
            Connection conn = qr.getDataSource().getConnection();
            try {
                conn.setAutoCommit(false);
                try {
                    list = qr.query(conn, sb.toString(), new BeanListHandler<DbopTaskBean>(DbopTaskBean.class));
                    for (DbopTaskBean task : list) {
                        String sql = "update " + tablename + " set status=4 where taskidd=?";
                        qr.update(conn, sql, task.getTaskIdd());
                    }
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                } finally {
                    conn.setAutoCommit(true);
                }
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            log.error("get DBOPTask error. SQLException: {}, query_sql: {}", e, sb.toString());
            throw e;
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

    public synchronized void updateStatus(int datatype) throws SQLException {
        List<DbopTaskBean> srchTaskBeans = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ").append(tablename).append(" where ROWID in (select RID from (select ROWID RID from ");
        sb.append(tablename).append(" where status=4 and datatype=?");
        sb.append(")) for update");

        try {
            Connection conn = qr.getDataSource().getConnection();
            try {
                conn.setAutoCommit(false);
                try {
                    srchTaskBeans = qr.query(conn, sb.toString(), new BeanListHandler<>(DbopTaskBean.class), datatype);
                    for (DbopTaskBean bean : srchTaskBeans) {
                        String sql1 = "update " + tablename + " set status=3 where taskidd=? and datatype=?";
                        qr.update(conn, sql1, bean.getTaskIdd(), bean.getDataType());
                    }
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                } finally {
                    conn.setAutoCommit(true);
                }
            } finally {
                conn.close();
            }

            log.debug("query_sql is {}", sb.toString());
        } catch (SQLException e) {
            log.error("SQLException: {}, query_sql:{}", e, sb.toString());
            throw e;
        }


//        sql.append(tablename).append(" set status=3 where status=4 and datatype=?");
//        try{
//            qr.update(sql.toString(), datatype);
//        } catch (SQLException e) {
//            log.error("update status error before shutting down");
//        }
    }
}
