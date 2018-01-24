package HAFPIS.DAO;

import HAFPIS.Utils.ConfigUtil;
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

    public List<SrchTaskBean> getSrchTaskBean(int status, int datatype, int tasktype, int queryNum) {
        List<SrchTaskBean> srchTaskBeans = null;
        StringBuilder sb = new StringBuilder();
        if (ConfigUtil.getConfig("database").toLowerCase().equals("sqlserver")) {
            sb.append("select * from (select top ").append(queryNum).append(" * from ");
            sb.append(tablename);
            sb.append(" where status=").append(status).append(" and datatype=").append(datatype).append(" and tasktype=").append(tasktype);
            sb.append(" order by priority desc, endtime asc) res");
        } else {
            sb.append("select * from (select * from ");
            sb.append(tablename);
            sb.append(" where status=").append(status).append(" and datatype=").append(datatype).append(" and tasktype=").append(tasktype);
            sb.append(" order by priority desc, endtime asc)");
            sb.append(" where rownum<=").append(queryNum);
        }

        try {
            srchTaskBeans = qr.query(sb.toString(), new BeanListHandler<>(SrchTaskBean.class));
            log.debug("query_sql is {}", sb.toString());
        } catch (SQLException e) {
            log.error("SQLException: {}, query_sql:{}", e, sb.toString());
        }
        return srchTaskBeans;
    }

    public synchronized List<SrchTaskBean> getList(String status, int[] datatypes, int[] tasktypes, String queryNum) {
        List<SrchTaskBean> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        if (ConfigUtil.getConfig("database").toLowerCase().equals("sqlserver")) {
            sb.append("select * from (select top ").append(queryNum).append(" * from ");
            int statusN = 0;
            try {
                statusN = Integer.parseInt(status);
            } catch (NumberFormatException e) {
                log.error("parse status error. status must be a number. status-{}, exception-{}", status, e);
            }
            sb.append(" where status=").append(statusN);
            sb.append(" and datatype in (");
            for (int datatype : datatypes) {
                if (datatype > 0) {
                    sb.append(datatype).append(",");
                }
            }
            sb.deleteCharAt(sb.length() - 1).append(")");
            sb.append(" and tasktype in (");
            for (int tasktype : tasktypes) {
                if (tasktype != 0) {
                    sb.append(tasktype).append(",");
                }
            }
            sb.deleteCharAt(sb.length() - 1).append(") res");
            sb.append(" order by priority desc, endtime asc");
        } else {
            sb.append("select * from ").append(tablename);
            int statusN = 0;
            try {
                statusN = Integer.parseInt(status);
            } catch (NumberFormatException e) {
                log.error("parse status error. status must be a number. status-{}, exception-{}", status, e);
            }
            sb.append(" where status=").append(statusN);
            sb.append(" and datatype in (");
            for (int datatype : datatypes) {
                if (datatype > 0) {
                    sb.append(datatype).append(",");
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
        }

        try {
            list = qr.query(sb.toString(), new BeanListHandler<>(SrchTaskBean.class));
            log.debug("query_sql is {}", sb.toString());
        } catch (SQLException e) {
            log.error("SQLException: {}, query_sql:{}", e, sb.toString());
        }
        return list;
    }

    public synchronized List<SrchTaskBean> getList(String status, int datatype, int tasktype, String queryNum) {
        List<SrchTaskBean> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        if (ConfigUtil.getConfig("database").toLowerCase().equals("sqlserver")) {
            sb.append("select * from (select top ").append(queryNum).append(" * from ").append(tablename);
            int statusN = 0;
            try {
                statusN = Integer.parseInt(status);
            } catch (NumberFormatException e) {
                log.error("parse status error. status must be a number. status-{}, exception-{}", status, e);
            }
            sb.append(" where status=").append(statusN);
            sb.append(" and datatype=").append(datatype);
            sb.append(" and tasktype=").append(tasktype);
            sb.append(" order by priority desc, begtime asc) res");
        } else {
            sb.append("select * from ( ");
            sb.append("select * from ").append(tablename);
            int statusN = 0;
            try {
                statusN = Integer.parseInt(status);
            } catch (NumberFormatException e) {
                log.error("parse status error. status must be a number. status-{}, exception-{}", status, e);
            }
            sb.append(" where status=").append(statusN);
            sb.append(" and datatype=").append(datatype);
            sb.append(" and tasktype=").append(tasktype);
            sb.append(" order by priority desc, begtime asc ");
            sb.append(") where rownum <= ").append(Integer.parseInt(queryNum));
        }

        try{
            list = qr.query(sb.toString(), new BeanListHandler<>(SrchTaskBean.class));
            log.debug("query sql is {}", sb.toString());
        } catch (SQLException e) {
            log.error("query_sql:{}, SQLException: ", sb.toString(), e);
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

    public synchronized void updateStatus(int[] datatypes, int[] tasktypes) {
        StringBuilder sb = new StringBuilder("update ");
        sb.append(tablename).append(" set status=3 where status=4 and datatype in (");
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
        try{
            qr.update(sb.toString());
        } catch (SQLException e) {
            log.error("update status error before program shutdown. ");
        }
    }
}
