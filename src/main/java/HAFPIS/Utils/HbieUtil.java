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
    private static int fpPort;
    private static int ppPort;
    private static int lppPort;
    private static int plpPort;
    private static int facePort;
    private static int irisPort;

    private HbieUtil() {
        try {
            log.info("HBIEClient init begin...");
            ipAddr = ConfigUtil.getConfig("host");
            fpPort = Integer.parseInt(ConfigUtil.getConfig("tenfp_port"));
            ppPort = Integer.parseInt(ConfigUtil.getConfig("fourpalm_port"));
            lppPort = Integer.parseInt(ConfigUtil.getConfig("latfp_port"));
            plpPort = Integer.parseInt(ConfigUtil.getConfig("latpalm_port"));
            facePort = Integer.parseInt(ConfigUtil.getConfig("face_port"));
            irisPort = Integer.parseInt(ConfigUtil.getConfig("iris_port"));

            hbie_FP = new HBIEClient<HSFPTenFp.Record>(ipAddr, fpPort);
            hbie_PP = new HBIEClient<HSFPFourPalm.Record>(ipAddr, ppPort);
            hbie_LPP = new HBIEClient<HSFPLatFp.Record>(ipAddr, lppPort);
            hbie_PLP = new HBIEClient<HSFPLatPalm.Record>(ipAddr, plpPort);
            hbie_FACE = new HBIEClient<THIDFace.Record>(ipAddr, facePort);
            hbie_IRIS = new HBIEClient<HSIris.Record>(ipAddr, irisPort);
            log.info("HBIEClient init end...");
        } catch (RemoteException e) {//TODO 如果初始化异常 应该停止运行重新启动
            log.error("remoteexception: ",e);
        } catch (NotBoundException e) {
            log.error("notboundexception: ", e);
        } catch (MalformedURLException e) {
            log.error("malformedURLexception: ", e);
        }
    }

//    static {
//        try {
//            log.info("HBIEClient init begin...");
//            ipAddr = ConfigUtil.getConfig("host");
//            fpPort = Integer.parseInt(ConfigUtil.getConfig("tenfp_port"));
//            ppPort = Integer.parseInt(ConfigUtil.getConfig("fourpalm_port"));
//            lppPort = Integer.parseInt(ConfigUtil.getConfig("latfp_port"));
//            plpPort = Integer.parseInt(ConfigUtil.getConfig("latpalm_port"));
//            facePort = Integer.parseInt(ConfigUtil.getConfig("face_port"));
//            irisPort = Integer.parseInt(ConfigUtil.getConfig("iris_port"));
//
//            hbie_FP = new HBIEClient<HSFPTenFp.Record>(ipAddr, fpPort);
//            hbie_PP = new HBIEClient<HSFPFourPalm.Record>(ipAddr, ppPort);
//            hbie_LPP = new HBIEClient<HSFPLatFp.Record>(ipAddr, lppPort);
//            hbie_PLP = new HBIEClient<HSFPLatPalm.Record>(ipAddr, plpPort);
//            hbie_FACE = new HBIEClient<THIDFace.Record>(ipAddr, facePort);
//            hbie_IRIS = new HBIEClient<HSIris.Record>(ipAddr, irisPort);
//            log.info("HBIEClient init end...");
//        } catch (RemoteException e) {
//            log.error("remoteexception: ",e);
//        } catch (NotBoundException e) {
//            log.error("notboundexception: ", e);
//        } catch (MalformedURLException e) {
//            log.error("malformedURLexception: ", e);
//        }
//    }

    private static final class HbieHolder{
        private static final HbieUtil HBIE_INSTANCE = new HbieUtil();
    }

    public static HbieUtil getInstance() {
        return HbieHolder.HBIE_INSTANCE;
    }
}
