package HAFPIS.DAO;

import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.PPTTRec;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by ZP on 2017/5/17.
 */
public class PPTTDAO {
    private final Logger log = LoggerFactory.getLogger(PPTTDAO.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private String tablename = null;

    public PPTTDAO(String tablename) {
        this.tablename = tablename;
    }

    public synchronized boolean updateRes(List<PPTTRec> list) {
        PPTTRec ppttRec = new PPTTRec();
        ppttRec = list.get(0);
        String taskid = ppttRec.taskid;
        String transno = ppttRec.transno;
        String probeid = ppttRec.probeid;
        Object[][] paramUsed = new Object[list.size()][11];
        int sum = 0;
        //先清理表中存在的重复的比对结果数据
        delete(taskid);
        //插入候选
        String ins_sql = "insert into " + tablename + " (TASKIDD, TRANSNO, PROBEID, DBID, CANDID, CANDRANK, SCORE, " +
                "SCORE01, SCORE05, SCORE06, SCORE10) values(?,?,?,?,?,?,?,?,?,?,?)";
        for (int i = 0; i < list.size(); i++) {
            int idx = 0;
            ppttRec = list.get(i);
            paramUsed[i][idx++] = taskid;
            paramUsed[i][idx++] = transno;
            paramUsed[i][idx++] = probeid;
            paramUsed[i][idx++] = ppttRec.dbid;
            paramUsed[i][idx++] = ppttRec.candid;
            paramUsed[i][idx++] = ppttRec.candrank;
            paramUsed[i][idx++] = (int) (ppttRec.score * 10000);
            for (int j = 0; j < 4; j++) {
                paramUsed[i][idx++] = (int) (ppttRec.ppscores[j] * 10000);
            }
        }
        try {
            sum = qr.batch(ins_sql, paramUsed).length;
        } catch (SQLException var2) {
            log.error("Insert into {} error. ProbeId={}, ExceptionMsg=", tablename, probeid, var2);
        }
        return sum == list.size();
    }

    public synchronized boolean delete(String taskid) {
        String del_sql = "delete from " + tablename + " where taskidd=?";
        try {
            return qr.update(del_sql, taskid) > 0;
        } catch (SQLException e) {
            log.error("Delete records before inserting records error: delSql={}, ExceptionMsg={}", del_sql, e);
            return false;
        }
    }
}
