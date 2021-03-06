package HAFPIS.DAO;

import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.FPLLRec;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by ZP on 2017/5/17.
 */
public class FPLLDAO {
    private final Logger log = LoggerFactory.getLogger(FPLLDAO.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private String tablename = null;

    public FPLLDAO(String tablename) {
        this.tablename = tablename;
    }

    public synchronized boolean updateRes(List<FPLLRec> list) {
        FPLLRec fpllRec = new FPLLRec();
        fpllRec = list.get(0);
        String taskid = fpllRec.taskid;
        String transno = fpllRec.transno;
        String probeid = fpllRec.probeid;
        Object[][] paramUsed = new Object[list.size()][8];
        int sum = 0;
        //先清理表中存在的重复的比对结果数据
        delete(taskid);
        //插入候选
        String ins_sql = "insert into " + tablename + " (TASKIDD, TRANSNO, PROBEID, DBID, CANDID, CANDRANK, POSITION, SCORE) VALUES(?,?,?,?,?,?,?,?)";
        for (int i = 0; i < list.size(); i++) {
            int idx = 0;
            fpllRec = list.get(i);
            paramUsed[i][idx++] = taskid;
            paramUsed[i][idx++] = transno;
            paramUsed[i][idx++] = probeid;
            paramUsed[i][idx++] = fpllRec.dbid;
            paramUsed[i][idx++] = fpllRec.candid;
            paramUsed[i][idx++] = fpllRec.candrank;
            paramUsed[i][idx++] = fpllRec.position;
            paramUsed[i][idx]   = (int) (fpllRec.score );
        }
        try {
            sum = qr.batch(ins_sql, paramUsed).length;
        } catch (SQLException var2) {
            log.error("Insert into {} error. ProbeId={}, ExceptionMsg=", tablename, probeid, var2);
        }
        return sum == list.size();
    }

    public synchronized boolean delete(String taskid) {
        String del_sql = "delete from " + tablename + " where taskidd=?" ;
        try {
            return qr.update(del_sql, taskid) > 0;
        } catch (SQLException e) {
            log.error("Delete records before inserting records error: delSql={}, ExceptionMsg={}", del_sql, e);
            return false;
        }
    }
}
