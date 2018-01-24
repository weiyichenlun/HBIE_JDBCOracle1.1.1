package HAFPIS.DAO;

import HAFPIS.Utils.QueryRunnerUtil;
import HAFPIS.domain.FaceRec;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by ZP on 2017/5/17.
 */
public class FaceTTDAO {
    private final Logger log = LoggerFactory.getLogger(FaceTTDAO.class);
    private QueryRunner qr = QueryRunnerUtil.getInstance();
    private String tablename = null;

    public FaceTTDAO(String tablename) {
        this.tablename = tablename;
    }

    public synchronized boolean updateRes(List<FaceRec> list) {
        FaceRec faceRec = new FaceRec();
        faceRec = list.get(0);
        String taskid = faceRec.taskid;
        String transno = faceRec.transno;
        String probeid = faceRec.probeid;
        Object[][] paramUsed = new Object[list.size()][10];
        int sum = 0;
        //先清理表中存在的重复的比对结果数据
        delete(taskid);
        //插入候选
        String ins_sql = "insert into " + tablename + " (TASKIDD, TRANSNO, PROBEID, DBID, CANDID, CANDRANK, SCORE, " +
                "SCORE01, SCORE02, SCORE03) VALUES(?,?,?,?,?,?,?,?,?,?)";
        for (int i = 0; i < list.size(); i++) {
            int idx = 0;
            faceRec = list.get(i);
            paramUsed[i][idx++] = taskid;
            paramUsed[i][idx++] = transno;
            paramUsed[i][idx++] = probeid;
            paramUsed[i][idx++] = faceRec.dbid;
            paramUsed[i][idx++] = faceRec.candid;
            paramUsed[i][idx++] = faceRec.candrank;
            if (faceRec.score <= 1 && faceRec.score > 0) {
                paramUsed[i][idx++] = (int) (faceRec.score * 10000);
                for (int j = 0; j < 3; j++) {
                    paramUsed[i][idx++] = (int) (faceRec.ffscores[j] * 10000);
                }
            } else if (faceRec.score > 1) {
                paramUsed[i][idx++] = (int) (faceRec.score);
                for (int j = 0; j < 3; j++) {
                    paramUsed[i][idx++] = (int) (faceRec.ffscores[j]);
                }
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
        String del_sql = "delete from " + tablename + " where taskidd=?" ;
        try {
            return qr.update(del_sql,taskid) > 0;
        } catch (SQLException e) {
            log.error("Delete records before inserting records error: delSql={}, ExceptionMsg={}", del_sql, e);
            return false;
        }
    }

}
