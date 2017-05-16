package HAFPIS;

import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.service.FpRecog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by ZP on 2017/5/12.
 */
public class HAFPIS_Main {
    private static Logger log = LoggerFactory.getLogger(HAFPIS_Main.class);
    public static void main(String[] args) {
        int num = 0;
        String interval = "1";
        String querynum = "10";
        String status = "3";
        String type = null;
        String tablename = null;
        Properties prop = new Properties();

        if (args == null) {
            log.info("请输入一个配置文件名称(例如HSFP.properties):  ");
            System.exit(-1);
        } else {
            String name = args[0];
            String temp = null;
            if (name.startsWith("-")) {
                if (name.startsWith("-cfg-file=")) {
                    temp = name.substring(name.indexOf(61) + 1);
                    prop = ConfigUtil.getProp(temp);
                } else {
                    int t = name.indexOf(61);
                    if (t == -1) {
                        temp = name;
                        prop = ConfigUtil.getProp(temp);
                    } else {
                        temp = name.substring(t + 1);
                        prop = ConfigUtil.getProp(temp);
                    }
                }
                type = (String) prop.get("type");
                interval = (String) prop.get("interval");
                querynum = (String) prop.get("querynum");
                status = (String) prop.get("status");
                tablename = (String) prop.get("tablename");

            }
            if (type == null) {
                log.error("没有指定type类型，无法启动程序");
                System.exit(-1);
            } else {
                String[] types = type.split("[,;\\s]+");
                if (types.length == 2) {
                    if ((types[0].equals("TT") && types[1].equals("LT")) || (types[0].equals("LT") && types[1].equals("TT"))) {
                        num = CONSTANTS.FPTTLT;
                    }
                    if ((types[0].equals("TL") && types[1].equals("LL")) || (types[0].equals("LL") && types[1].equals("TL"))) {
                        num = CONSTANTS.FPTLLL;
                    }
                    if ((types[0].equals("P2P") && types[1].equals("L2P")) || (types[0].equals("L2P") && types[1].equals("P2P"))) {
                        num = CONSTANTS.PPTTLT;
                    }
                    if ((types[0].equals("P2L") && types[1].equals("L2L")) || (types[0].equals("L2L") && types[1].equals("P2L"))) {
                        num = CONSTANTS.PPTLLL;
                    }
                } else if (types.length == 1) {
                    switch (types[0]) {
                        case "TT":
                            num = CONSTANTS.FPTT;
                            break;
                        case "LT":
                            num = CONSTANTS.FPLT;
                            break;
                        case "TL":
                            num = CONSTANTS.FPTL;
                            break;
                        case "LL":
                            num = CONSTANTS.FPLL;
                            break;
                        case "P2P":
                            num = CONSTANTS.PPTT;
                            break;
                        case "L2P":
                            num = CONSTANTS.PPLT;
                            break;
                        case "P2L":
                            num = CONSTANTS.PPTL;
                            break;
                        case "L2L":
                            num = CONSTANTS.PPLL;
                            break;
                        case "FACE":
                            num = CONSTANTS.FACE;
                            break;
                        case "IRIS":
                            num = CONSTANTS.IRIS;
                            break;
                        default:
                            log.warn("type error.");
                            break;
                    }
                }
            }

            switch (num) {
                case CONSTANTS.FPTT:
                case CONSTANTS.FPLT:
                case CONSTANTS.FPTTLT:
                    String FPTT_threshold = (String) prop.get("FPTT_threshold");
                    String FPLT_threshold = (String) prop.get("FPLT_threshold");
                    String FPTT_tablename = (String) prop.get("FPTT_tablename");
                    String FPLT_tablename = (String) prop.get("FPLT_tablename");
                    FpRecog fpRecog = new FpRecog();
                    fpRecog.setType(num);
                    fpRecog.setInterval(interval);
                    fpRecog.setQueryNum(querynum);
                    fpRecog.setStatus(status);
                    fpRecog.setTablename(tablename);
                    fpRecog.setFPTT_threshold(Float.parseFloat(FPTT_threshold));
                    fpRecog.setFPTT_tablename(FPTT_tablename);
                    fpRecog.setFPLT_threshold(Float.parseFloat(FPLT_threshold));
                    fpRecog.setFPLT_tablename(FPLT_tablename);
                    Thread fpThread = new Thread(fpRecog, "FPThread");
                    fpThread.start();
                    break;

            }
        }
    }
}
