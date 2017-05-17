package HAFPIS;

import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.service.FaceRecog;
import HAFPIS.service.FpRecog;
import HAFPIS.service.IrisRecog;
import HAFPIS.service.LatFpRecog;
import HAFPIS.service.LatPalmRecog;
import HAFPIS.service.PalmRecog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 主函数
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
            System.out.println(HAFPIS_Main.class.getResource(""));
            System.out.println(HAFPIS_Main.class.getResource("/"));
            prop = ConfigUtil.getProp(args);
            type = (String) prop.get("type");
            interval = (String) prop.get("interval");
            querynum = (String) prop.get("querynum");
            status = (String) prop.get("status");
            tablename = (String) prop.get("tablename");
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
                case CONSTANTS.FPTL:
                case CONSTANTS.FPLL:
                case CONSTANTS.FPTLLL:
                    String FPTL_threshold = (String) prop.get("FPTL_threshold");
                    String FPLL_threshold = (String) prop.get("FPLL_threshold");
                    String FPTL_tablename = (String) prop.get("FPTL_tablename");
                    String FPLL_tablename = (String) prop.get("FPLL_tablename");
                    LatFpRecog latFpRecog = new LatFpRecog();
                    latFpRecog.setType(num);
                    latFpRecog.setInterval(interval);
                    latFpRecog.setQueryNum(querynum);
                    latFpRecog.setStatus(status);
                    latFpRecog.setTablename(tablename);
                    latFpRecog.setFPTL_threshold(Float.parseFloat(FPTL_threshold));
                    latFpRecog.setFPTL_tablename(FPTL_tablename);
                    latFpRecog.setFPLL_threshold(Float.parseFloat(FPLL_threshold));
                    latFpRecog.setFPLL_tablename(FPLL_tablename);
                    Thread latfpThread = new Thread(latFpRecog, "LatFPThread");
                    latfpThread.start();
                    break;
                case CONSTANTS.PPTT:
                case CONSTANTS.PPLT:
                case CONSTANTS.PPTTLT:
                    String PPTT_threshold = (String) prop.get("PMTT_threshold");
                    String PPLT_threshold = (String) prop.get("PMLT_threshold");
                    String PPTT_tablename = (String) prop.get("PMTT_tablename");
                    String PPLT_tablename = (String) prop.get("PMLT_tablename");
                    PalmRecog palmRecog = new PalmRecog();
                    palmRecog.setType(num);
                    palmRecog.setInterval(interval);
                    palmRecog.setQueryNum(querynum);
                    palmRecog.setStatus(status);
                    palmRecog.setTablename(tablename);
                    palmRecog.setPPTT_tablename(PPTT_tablename);
                    palmRecog.setPPTT_threshold(Float.parseFloat(PPTT_threshold));
                    palmRecog.setPPLT_tablename(PPLT_tablename);
                    palmRecog.setPPLT_threshold(Float.parseFloat(PPLT_threshold));
                    Thread palmThread = new Thread(palmRecog, "PalmThread");
                    palmThread.start();
                    break;
                case CONSTANTS.PPTL:
                case CONSTANTS.PPLL:
                case CONSTANTS.PPTLLL:
                    String PPTL_threshold = (String) prop.get("PMTL_threshold");
                    String PPLL_threshold = (String) prop.get("PMLL_threshold");
                    String PPTL_tablename = (String) prop.get("PMTL_tablename");
                    String PPLL_tablename = (String) prop.get("PMLL_tablename");
                    LatPalmRecog latPalmRecog = new LatPalmRecog();
                    latPalmRecog.setType(num);
                    latPalmRecog.setInterval(interval);
                    latPalmRecog.setQueryNum(querynum);
                    latPalmRecog.setStatus(status);
                    latPalmRecog.setTablename(tablename);
                    latPalmRecog.setPPTL_tablename(PPTL_tablename);
                    latPalmRecog.setPPTL_threshold(Float.parseFloat(PPTL_threshold));
                    latPalmRecog.setPPLL_tablename(PPLL_tablename);
                    latPalmRecog.setPPLL_threshold(Float.parseFloat(PPLL_threshold));
                    Thread latpalmThread = new Thread(latPalmRecog, "LatpalmThread");
                    latpalmThread.start();
                    break;
                case CONSTANTS.FACE:
                    String FaceTT_threshold = (String) prop.get("FaceTT_threshold");
                    String FaceTT_tablename = (String) prop.get("FaceTT_tablename");
                    FaceRecog faceRecog = new FaceRecog();
                    faceRecog.setType(num);
                    faceRecog.setInterval(interval);
                    faceRecog.setQueryNum(querynum);
                    faceRecog.setStatus(status);
                    faceRecog.setTablename(tablename);
                    faceRecog.setFaceTT_tablename(FaceTT_tablename);
                    faceRecog.setFaceTT_threshold(Float.parseFloat(FaceTT_threshold));
                    Thread faceThread = new Thread(faceRecog, "FaceThread");
                    faceThread.start();
                    break;
                case CONSTANTS.IRIS:
                    String IrisTT_threshold = (String) prop.get("IrisTT_threshold");
                    String IrisTT_tablename = (String) prop.get("IrisTT_tablename");
                    IrisRecog irisRecog = new IrisRecog();
                    irisRecog.setType(num);
                    irisRecog.setInterval(interval);
                    irisRecog.setQueryNum(querynum);
                    irisRecog.setStatus(status);
                    irisRecog.setTablename(tablename);
                    irisRecog.setIrisTT_tablename(IrisTT_tablename);
                    irisRecog.setIrisTT_threshold(Float.parseFloat(IrisTT_threshold));
                    Thread irisThread = new Thread(irisRecog, "FaceThread");
                    irisThread.start();
                    break;
            }
        }
    }
}
