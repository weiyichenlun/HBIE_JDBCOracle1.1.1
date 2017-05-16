package HAFPIS.DAO;

import HAFPIS.Utils.DateUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import org.apache.commons.dbutils.QueryRunner;
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

    public synchronized void update(String taskidd, int status, String exptmsg) {
        StringBuilder sb = new StringBuilder();
        List<Object> param = new ArrayList<>();
        sb.append("update ").append(tablename).append(" set");
        sb.append(" status=?,");
        param.add(status);
        sb.append(" endtime=?");
        String date = DateUtil.getFormatDate(System.currentTimeMillis());
        param.add(DateUtil.getFormatDate(System.currentTimeMillis()));
        if (status < 0) {
            sb.append(", exptmsg=?");
            param.add(exptmsg);
        }
        sb.append(" where taskidd=?");
        param.add(taskidd);
        try {
            qr.update(sb.toString(), status,date, taskidd );
        } catch (SQLException e) {
            log.error("UPDATE DB error: ", e);
        }
    }
}
