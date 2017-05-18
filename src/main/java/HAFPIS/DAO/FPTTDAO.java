package HAFPIS.DAO;

import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.FPTTRec;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * 指纹查重DAO
 * Created by ZP on 2017/5/16.
 */
public class FPTTDAO {
    private final Logger log = LoggerFactory.getLogger(FPTTDAO.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private String tablename = null;

    public FPTTDAO(String tablename) {
        this.tablename = tablename;
    }


    public synchronized boolean updateRes(List<FPTTRec> list) {
        FPTTRec fpttRec = new FPTTRec();
        fpttRec = list.get(0);
        String taskid = fpttRec.taskid;
        String transno = fpttRec.transno;
        String probeid = fpttRec.probeid;
        Object[][] paramUsed = new Object[list.size()][27];
        int sum = 0;
        //先清理表中存在的重复的比对结果数据
        delete(taskid);
        //插入候选
        String ins_sql = "insert into " + tablename + " (TASKIDD, TRANSNO, PROBEID, DBID, CANDID, CANDRANK, SCORE, SCORE01, SCORE02, " +
                "SCORE03, SCORE04, SCORE05, SCORE06, SCORE07, SCORE08, SCORE09, SCORE10, SCORE11, SCORE12, SCORE13, SCORE14, " +
                "SCORE15, SCORE16, SCORE17, SCORE18, SCORE19, SCORE20) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        for (int i = 0; i < list.size(); i++) {
            int idx = 0;
            fpttRec = list.get(i);
            paramUsed[i][idx++] = taskid;
            paramUsed[i][idx++] = transno;
            paramUsed[i][idx++] = probeid;
            paramUsed[i][idx++] = fpttRec.dbid;
            paramUsed[i][idx++] = fpttRec.candid;
            paramUsed[i][idx++] = fpttRec.candrank;
            paramUsed[i][idx++] = (int) (fpttRec.score * 10000);
            for (int j = 0; j < 10; j++) {
                paramUsed[i][idx++] = (int) (fpttRec.rpscores[j] * 10000);
            }
            for (int j = 0; j < 10; j++) {
                paramUsed[i][idx++] = (int) (fpttRec.fpscores[j] * 10000);
            }
        }
        try {
            sum = qr.batch(ins_sql, paramUsed).length;
        } catch (SQLException var2) {
            log.error("Insert into {} error. ProbeId={}, ExceptionMsg={}", tablename, probeid, var2);
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
