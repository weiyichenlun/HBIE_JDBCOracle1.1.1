package HAFPIS.Utils;

import com.hisign.bie.HBIEClient;
import com.hisign.bie.hsfp.HSFPFourPalm;
import com.hisign.bie.hsfp.HSFPLatFp;
import com.hisign.bie.hsfp.HSFPLatPalm;
import com.hisign.bie.hsfp.HSFPTenFp;
import com.hisign.bie.iris.HSIris;
import com.hisign.bie.thid.THIDFace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * HBIE客户端初始化工具类
 * Created by ZP on 2017/5/12.
 */
public class HbieUtil {
    private static final Logger log = LoggerFactory.getLogger(HbieUtil.class);
    public  HBIEClient<HSFPTenFp.Record> hbie_FP;
    public  HBIEClient<HSFPFourPalm.Record> hbie_PP;
    public  HBIEClient<HSFPLatFp.Record> hbie_LPP;
    public  HBIEClient<HSFPLatPalm.Record> hbie_PLP;
    public  HBIEClient<THIDFace.Record> hbie_FACE;
    public  HBIEClient<HSIris.Record> hbie_IRIS;
    private static String ipAddr;
    private static String[] ipAddrs;
    private static int fpPort;
    private static int ppPort;
    private static int lppPort;
    private static int plpPort;
    private static int facePort;
    private static int irisPort;

    private HbieUtil() {
        try {
            if (ipAddrs.length > 0) {
                int len = ipAddrs.length;
                String[] cpIpAddrs = new String[len];
                if (fpPort != -1) {
                    for (int i = 0; i < len; i++) {
                        cpIpAddrs[i] = ipAddrs[i] + ":" + fpPort;
                    }
                    hbie_FP = new HBIEClient<>(cpIpAddrs);
                } else {
                    hbie_FP = null;
                }
                cpIpAddrs = new String[len];
                if (lppPort != -1) {
                    for (int i = 0; i < len; i++) {
                        cpIpAddrs[i] = ipAddrs[i] + ":" + lppPort;
                    }
                    hbie_LPP = new HBIEClient<>(cpIpAddrs);
                } else {
                    hbie_LPP = null;
                }

                cpIpAddrs = new String[len];
                if (ppPort != -1) {
                    for (int i = 0; i < len; i++) {
                        cpIpAddrs[i] = ipAddrs[i] + ":" + ppPort;
                    }
                    hbie_PP = new HBIEClient<>(cpIpAddrs);
                } else {
                    hbie_PP = null;
                }

                cpIpAddrs = new String[len];
                if (plpPort != -1) {
                    for (int i = 0; i < len; i++) {
                        cpIpAddrs[i] = ipAddrs[i] + ":" + plpPort;
                    }
                    hbie_PLP = new HBIEClient<>(cpIpAddrs);
                } else {
                    hbie_PLP = null;
                }

                cpIpAddrs = new String[len];
                if (facePort != -1) {
                    for (int i = 0; i < len; i++) {
                        cpIpAddrs[i] = ipAddrs[i] + ":" + facePort;
                    }
                    hbie_FACE = new HBIEClient<>(cpIpAddrs);
                } else {
                    hbie_FACE = null;
                }

                cpIpAddrs = new String[len];
                if (irisPort != -1) {
                    for (int i = 0; i < len; i++) {
                        cpIpAddrs[i] = ipAddrs[i] + ":" + irisPort;
                    }
                    hbie_IRIS = new HBIEClient<>(cpIpAddrs);
                } else {
                    hbie_IRIS = null;
                }
            } else {
                log.info("host config error");
            }
            log.info("HBIEClient init end...");
        } catch (RemoteException e) {//TODO 如果初始化异常 应该停止运行重新启动
            log.error("remoteexception: ",e);
        } catch (NotBoundException e) {
            log.error("notboundexception: ", e);
        } catch (MalformedURLException e) {
            log.error("malformedURLexception: ", e);
        }
    }


    public static final class HbieHolder{
        private static final HbieUtil HBIE_INSTANCE = new HbieUtil();
        public static final String[] ips = ipAddrs;
        public static final int tpp_port = fpPort;
        public static final int lpp_port = lppPort;
        public static final int plm_port = ppPort;
        public static final int plp_port = plpPort;
        public static final int face_port = facePort;
        public static final int iris_port = irisPort;
    }

    static {
        log.info("HBIEClient init begin...");
        ipAddr = ConfigUtil.getConfig("host");
        ipAddrs = ipAddr.split(",");
        log.info("host is " + ipAddr);
        try {
            String temp = ConfigUtil.getConfig("tenfp_port");
            if (temp == null) {
                fpPort = -1;
            } else {
                fpPort = Integer.parseInt(temp);
            }
        } catch (NumberFormatException e) {
            log.warn("tenfp_port in hbie.cfg.properties config error. {}\n Use default tenfp_port 1099", fpPort);
            fpPort = 1099;
        }
        try {
            String temp = ConfigUtil.getConfig("fourpalm_port");
            if (temp == null) {
                ppPort = -1;
            } else {
                ppPort = Integer.parseInt(temp);
            }
        } catch (NumberFormatException e) {
            log.warn("fourpalm_port in hbie.cfg.properties config error. {}\n Use default fourpalm_port 1100", ppPort);
            ppPort = 1100;
        }
        try {
            String temp = ConfigUtil.getConfig("latfp_port");
            if (temp == null) {
                lppPort = -1;
            } else {
                lppPort = Integer.parseInt(temp);
            }
        } catch (NumberFormatException e) {
            log.warn("latfp_port in hbie.cfg.properties config error. {}\n Use default latfp_port 1101", lppPort);
            lppPort = 1101;
        }
        try {
            String temp = ConfigUtil.getConfig("latpalm_port");
            if (temp == null) {
                plpPort = -1;
            } else {
                plpPort = Integer.parseInt(temp);
            }
        } catch (NumberFormatException e) {
            log.warn("latpalm_port in hbie.cfg.properties config error. {}\n Use default latpalm 1102", plpPort);
            plpPort = 1102;
        }
        try {
            String temp = ConfigUtil.getConfig("face_port");
            if (temp == null) {
                facePort = -1;
            } else {
                facePort = Integer.parseInt(temp);
            }
        } catch (NumberFormatException e) {
            log.warn("face_port in hbie.cfg.properties config error. {}\n Use default face_port 1103", facePort);
            facePort = 1103;
        }
        try {
            String temp = ConfigUtil.getConfig("iris_port");
            if (temp == null) {
                irisPort = -1;
            } else {
                irisPort = Integer.parseInt(temp);
            }
        } catch (NumberFormatException e) {
            log.warn("iris_port in hbie.cfg.properties config error. {}\n Use default iris_port 1104", irisPort);
            irisPort = 1104;
        }
    }




    public static HbieUtil getInstance() {
        return HbieHolder.HBIE_INSTANCE;
    }

    public static void main(String[] args) {
        String ip = "172.16.2.12,172.16.2.171";
        String[] ips = ip.split(",");
        if (ips.length > 1) {
            StringBuilder sb = new StringBuilder();
            for (String ip1 : ips) {
                sb.append(ip1).append(":").append(1099).append(",");
            }
            sb.setLength(sb.length() - 1);
            System.out.println(sb.toString());
            sb.setLength(0);

            for (String ip1 : ips) {
                sb.append(ip1).append(":").append(1098).append(",");
            }
            sb.setLength(sb.length() - 1);
            System.out.println(sb.toString());
        }
    }
}
