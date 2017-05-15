package HAFPIS.DAO;

import HAFPIS.Utils.DateUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

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

    public void update(String taskidd, int status, String exptmsg) {
        StringBuilder sb = new StringBuilder();
        sb.append("update ").append(tablename).append(" set ");
        sb.append(", status=").append(status);
        sb.append(", endtime=").append(DateUtil.getFormatDate(System.currentTimeMillis()));
        if (status < 0) {
            sb.append(", exptmsg=").append(exptmsg);
        }
        sb.append(" where taskidd=").append(taskidd);

        try {
            qr.update(sb.toString());
        } catch (SQLException e) {
            log.error("UPDATE DB error: {}", e);
        }
    }
}
