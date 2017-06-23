package HAFPIS.DAO;

import HAFPIS.Utils.ConfigUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Created by ZP on 2017/6/23.
 */
public class PLPDAO {
    private static final Logger log = LoggerFactory.getLogger(TPPDAO.class);
    private static QueryRunner qr = QueryRunnerUtil.getInstance();
    private String tablename;

    public PLPDAO(String tablename) {
        this.tablename = tablename;
    }



    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public static int getDbId(String latentid) {
        String sql = "SELECT DBID FROM " + ConfigUtil.getConfig("plp_table") + " WHERE LATENTID=?";
        try{
            return qr.query(sql, rs -> {
                if (rs.next()) {
                    return rs.getInt("dbid");
                }
                return 0;
            }, latentid);
        } catch (SQLException e) {
            log.error("get dbid error and use default dbid: {}. sql: {}, probeid: {}, exception: ",0, sql, latentid, e);
        }
        return 0;
    }
}
