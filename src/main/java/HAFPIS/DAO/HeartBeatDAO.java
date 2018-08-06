package HAFPIS.DAO;

import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.HeartBeatBean;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * 描述：
 * 作者：ZP
 * 创建时间:2018/3/19
 * 最后修改时间:2018/3/19
 */
public class HeartBeatDAO {
    private Logger log = LoggerFactory.getLogger(HeartBeatDAO.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private String tableName = null;

    public HeartBeatDAO(String tableName) {
        this.tableName = tableName;
    }

    public HeartBeatBean queryEarliest() {
        String sql = "select * from (select * from " + this.tableName + " order by updatetime asc) where rownum <= 1";
        return query(sql);
    }

    public HeartBeatBean queryLatest() {
        String sql = null;
        if (ConfigUtil.getConfig("database").toLowerCase().equals("sqlserver")) {
            sql = "select * from (select top 1 * from " + this.tableName + " order by updatetime desc) res";
        } else {
            sql = "select * from (select * from " + this.tableName + " order by updatetime desc) where rownum <= 1";
        }
        return query(sql);
    }

    public HeartBeatBean query(String sql) {
        try {
            return this.qr.query(sql, new BeanHandler<>(HeartBeatBean.class));
        } catch (SQLException e) {
            log.error("Query Heartbeat table error. ", e);
            return null;
        }
    }

    public List<HeartBeatBean> queryAll() {
        String sql = "select * from " + this.tableName + " where rankno = 1";
        try {
            return this.qr.query(sql, new BeanListHandler<>(HeartBeatBean.class));
        } catch (SQLException e) {
            log.error("query heartbeat table error.", e);
            return null;
        }
    }

    public List<HeartBeatBean> querySome(int num) {
        String sql = null;
        if (ConfigUtil.getConfig("database").toLowerCase().equals("sqlserver")) {
            sql = "select * from (select top " + num + " * from " + this.tableName + " order by updatetime desc, rankno=1) res";
        } else {
            sql = "select * from (select * from " + this.tableName + " order by updatetime desc, rankno=1 ) where rownum <= " + num;
        }
        try {
            return this.qr.query(sql, new BeanListHandler<>(HeartBeatBean.class));
        } catch (SQLException e) {
            log.error("Query Heartbeat table error. ", e);
            return null;
        }
    }

    public synchronized boolean updateRankno(long time, String pid) {
        String sql = "update " + this.tableName + " set rankno=0, updatetime=? where instancename=?";
        try {
            return this.qr.update(sql, time, pid) > 0;
        } catch (SQLException e) {
            log.error("Update rankno error. pid: {}", pid, e);
            return false;
        }
    }

    public synchronized boolean update(long time, String pid) {
        String sql = "update " + this.tableName + " set updatetime=? where instancename=?";
        try {
            return this.qr.update(sql, time, pid) > 0;
        } catch (SQLException e) {
            log.error("Update Heartbeat table error. ", e);
            return false;
        }
    }

    public synchronized boolean delete(String pid) {
        String sql = "delete from " + this.tableName + " where instancename=?";
        try {
            return this.qr.update(sql, pid) > 0;
        } catch (SQLException e) {
            log.error("Delete from HeartBeat table error. instancename: {}", pid, e);
            return false;
        }
    }

    public synchronized boolean insert(String pid, int rank, long time) throws SQLException {
        String sql = "insert into " + this.tableName + " (instancename, rankno, updatetime, updatetime1) " +
                "values (?, ?, ?, to_char(systimestamp, 'YYYY-MM-DD HH24:MI:SS.FF'))";
        try {
            return this.qr.update(sql, pid, rank, time) > 0;
        } catch (SQLException e) {
            log.error("Insert into HeartBeat table error. instancename: {}", pid, e);
            throw e;
        }
    }
}
