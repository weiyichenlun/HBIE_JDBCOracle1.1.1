package HAFPIS.DAO;

import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Created by ZP on 2017/5/19.
 */
public class TPPDAO {
    private static final Logger log = LoggerFactory.getLogger(TPPDAO.class);
    private static QueryRunner qr = QueryRunnerUtil.getInstance();
    private String tablename;

    public TPPDAO(String tablename) {
        this.tablename = tablename;
    }



    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public synchronized String getImgMask(String personid) {
        StringBuilder sb = new StringBuilder();
        sb.append("select imgmask from ").append(tablename).append(" where personid=?");
        try {
            return qr.query(sb.toString(), rs -> {
                String mask1 = null;
                while (rs.next()) {
                    mask1 = rs.getString(0);
                }
                return mask1;
            }, personid);
        } catch (SQLException e) {
            log.error(" get imgmask error. sql: {}, probeid: {}, exception: ", sb.toString(), personid, e);
        }
        return null;
    }

    public static int getDbId(String personid) {
        String sql = "SELECT DBID FROM " + ConfigUtil.getConfig("tpp_table") + " WHERE PERSONID=?";
        try{
            return qr.query(sql, rs -> {
                if (rs.next()) {
                    return rs.getInt("dbid");
                }
                return 0;
            }, personid);
        } catch (SQLException e) {
            log.error("get dbid error and use default dbid: {}. sql: {}, probeid: {}, exception: ",0, sql, personid, e);
        }
        return 0;
    }
}
