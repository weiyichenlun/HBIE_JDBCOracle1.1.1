package HAFPIS.DAO;

import HAFPIS.Utils.QueryRunnerUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by ZP on 2017/5/19.
 */
public class PINFODAO {
    private final Logger log = LoggerFactory.getLogger(PINFODAO.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private String tablename;

    public PINFODAO(String tablename) {
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
            String mask = qr.query(sb.toString(), new ResultSetHandler<String>() {
                @Override
                public String handle(ResultSet rs) throws SQLException {
                    String mask = null;
                    while (rs.next()) {
                        mask = rs.getString(0);
                    }
                    return mask;
                }
            }, personid);
            return mask;
        } catch (SQLException e) {
            log.error(" get imgmask error. sql: {}, probeid: {}, exception: {}", sb.toString(), personid, e);
        }
        return null;
    }
}
