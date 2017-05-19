package HAFPIS;

import HAFPIS.Utils.CONSTANTS;
import HAFPIS.Utils.ConfigUtil;
import HAFPIS.service.DBOP_LPP;
import HAFPIS.service.DBOP_PLP;
import HAFPIS.service.DBOP_TPP;
import HAFPIS.service.FaceRecog;
import HAFPIS.service.FpRecog;
import HAFPIS.service.IrisRecog;
import HAFPIS.service.LatFpRecog;
import HAFPIS.service.LatPalmRecog;
import HAFPIS.service.OneToF_FPLL;
import HAFPIS.service.OneToF_FPTT;
import HAFPIS.service.OneToF_Face;
import HAFPIS.service.OneToF_Iris;
import HAFPIS.service.OneToF_PPTT;
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
                        case "FPTT_1ToF":
                            num = CONSTANTS.FPTT1TOF;
                            break;
                        case "FPLL_1ToF":
                            num = CONSTANTS.FPLL1TOF;
                            break;
                        case "PPTT_1ToF":
                            num = CONSTANTS.PPTT1TOF;
                            break;
                        case "PPLL_1ToF":
                            num = CONSTANTS.PPLL1TOF;
                            break;
                        case "FACE_1ToF":
                            num = CONSTANTS.FACE1TOF;
                            break;
                        case "IRIS_1ToF":
                            num = CONSTANTS.IRIS1TOF;
                            break;
                        case "DBOP_TPP":
                            num = CONSTANTS.DBOP_TPP;
                            break;
                        case "DBOP_LPP":
                            num = CONSTANTS.DBOP_LPP;
                            break;
                        case "DBOP_PLP":
                            num = CONSTANTS.DBOP_PLP;
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
                case CONSTANTS.FPTT1TOF:
                    String tablename_FPTT = (String) prop.get("result_tablename");
                    OneToF_FPTT oneToF_fptt = new OneToF_FPTT();
                    oneToF_fptt.setType(num);
                    oneToF_fptt.setInterval(interval);
                    oneToF_fptt.setQueryNum(querynum);
                    oneToF_fptt.setStatus(status);
                    oneToF_fptt.setTablename(tablename);
                    oneToF_fptt.setFPTT_tablename(tablename_FPTT);
                    Thread oneToF_FPTT_Thread = new Thread(oneToF_fptt, "OneToF_FPTT_Thread");
                    oneToF_FPTT_Thread.start();
                    break;
                case CONSTANTS.FPLL1TOF:
                    String tablename_FPLL = (String) prop.get("result_tablename");
                    OneToF_FPLL oneToF_fpll = new OneToF_FPLL();
                    oneToF_fpll.setType(num);
                    oneToF_fpll.setInterval(interval);
                    oneToF_fpll.setQueryNum(querynum);
                    oneToF_fpll.setStatus(status);
                    oneToF_fpll.setTablename(tablename);
                    oneToF_fpll.setFPLL_tablename(tablename_FPLL);
                    Thread oneToF_FPLL_Thread = new Thread(oneToF_fpll, "OneToF_FPll_Thread");
                    oneToF_FPLL_Thread.start();
                    break;
                case CONSTANTS.PPTT1TOF:
                    String tablename_PPTT= (String) prop.get("result_tablename");
                    OneToF_PPTT oneToF_pptt = new OneToF_PPTT();
                    oneToF_pptt.setType(num);
                    oneToF_pptt.setInterval(interval);
                    oneToF_pptt.setQueryNum(querynum);
                    oneToF_pptt.setStatus(status);
                    oneToF_pptt.setTablename(tablename);
                    oneToF_pptt.setPPTT_tablename(tablename_PPTT);
                    Thread oneToF_PPTT_Thread = new Thread(oneToF_pptt, "OneToF_PPTT_Thread");
                    oneToF_PPTT_Thread.start();
                    break;
                case CONSTANTS.PPLL1TOF:
                    break;
                case CONSTANTS.FACE1TOF:
                    String tablename_Face = (String) prop.get("result_tablename");
                    OneToF_Face oneToF_face = new OneToF_Face();
                    oneToF_face.setType(num);
                    oneToF_face.setInterval(interval);
                    oneToF_face.setQueryNum(querynum);
                    oneToF_face.setStatus(status);
                    oneToF_face.setTablename(tablename);
                    oneToF_face.setFace_tablename(tablename_Face);
                    Thread oneToF_Face_Thread = new Thread(oneToF_face, "OneToF_Face_Thread");
                    oneToF_Face_Thread.start();
                    break;
                case CONSTANTS.IRIS1TOF:
                    String tablename_Iris = (String) prop.get("result_tablename");
                    OneToF_Iris oneToF_iris = new OneToF_Iris();
                    oneToF_iris.setType(num);
                    oneToF_iris.setInterval(interval);
                    oneToF_iris.setQueryNum(querynum);
                    oneToF_iris.setStatus(status);
                    oneToF_iris.setTablename(tablename);
                    oneToF_iris.setIris_tablename(tablename_Iris);
                    Thread oneToF_Iris_Thread = new Thread(oneToF_iris, "OneToF_Iris_Thread");
                    oneToF_Iris_Thread.start();
                    break;
                case CONSTANTS.DBOP_TPP:
                    String tablename_pinfo = (String) prop.get("tablename_pinfo");
                    DBOP_TPP dbop_tpp = new DBOP_TPP();
                    dbop_tpp.setType(num);
                    dbop_tpp.setInterval(interval);
                    dbop_tpp.setQueryNum(querynum);
                    dbop_tpp.setStatus(status);
                    dbop_tpp.setTablename(tablename);
                    dbop_tpp.setTablename_pinfo(tablename_pinfo);
                    Thread dbop_tpp_thread = new Thread(dbop_tpp, "Dbop_TPP_Thread");
                    dbop_tpp_thread.start();
                    break;
                case CONSTANTS.DBOP_LPP:
                    DBOP_LPP dbop_lpp = new DBOP_LPP();
                    dbop_lpp.setType(num);
                    dbop_lpp.setInterval(interval);
                    dbop_lpp.setQueryNum(querynum);
                    dbop_lpp.setStatus(status);
                    dbop_lpp.setTablename(tablename);
                    Thread dbop_lpp_thread = new Thread(dbop_lpp, "Dbop_LPP_Thread");
                    dbop_lpp_thread.start();
                    break;
                case CONSTANTS.DBOP_PLP:
                    DBOP_PLP dbop_plp = new DBOP_PLP();
                    dbop_plp.setType(num);
                    dbop_plp.setInterval(interval);
                    dbop_plp.setQueryNum(querynum);
                    dbop_plp.setStatus(status);
                    dbop_plp.setTablename(tablename);
                    Thread dbop_plp_thread = new Thread(dbop_plp, "Dbop_PLP_Thread");
                    dbop_plp_thread.start();
                    break;
            }
        }
    }
}
