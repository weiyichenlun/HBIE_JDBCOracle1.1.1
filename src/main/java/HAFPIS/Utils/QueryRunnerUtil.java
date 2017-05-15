package HAFPIS.Utils;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 利用jvm机制特点，采用静态内部类方式来实现线程安全的单例模式
 * Created by ZP on 2017/5/15.
 */
public class QueryRunnerUtil {
    private static final Logger log = LoggerFactory.getLogger(QueryRunnerUtil.class);
    private static BasicDataSource ds  = new BasicDataSource();
    static {
        log.info("begin to initialize queryrunner...");
        String driver = ConfigUtil.getConfig("driver");
        String url = ConfigUtil.getConfig("url");
        String usr = ConfigUtil.getConfig("usr");
        String pwd = ConfigUtil.getConfig("pwd");
        ds.setDriverClassName(driver);
        ds.setUrl(url);
        ds.setUsername(usr);
        ds.setPassword(pwd);
        ds.setTestOnBorrow(true);
        log.info("queryrunner initialize finish...");
    }

    private QueryRunnerUtil(){}

    public static QueryRunner getInstance() {
        return QueryRunnerHolder.INSTANCE;
    }

    private static final class QueryRunnerHolder{
        private static final QueryRunner INSTANCE = new QueryRunner(ds);
    }
}
