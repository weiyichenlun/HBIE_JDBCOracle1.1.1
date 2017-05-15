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
    public static HBIEClient<HSFPTenFp.Record> hbie_FP;
    public static HBIEClient<HSFPFourPalm.Record> hbie_PP;
    public static HBIEClient<HSFPLatFp.Record> hbie_LPP;
    public static HBIEClient<HSFPLatPalm.Record> hbie_PLP;
    public static HBIEClient<THIDFace.Record> hbie_FACE;
    public static HBIEClient<HSIris.Record> hbie_IRIS;
    private static String ipAddr;
    private static int fpPort;
    private static int ppPort;
    private static int lppPort;
    private static int plpPort;
    private static int facePort;
    private static int irisPort;

    static {
        try {
            log.info("HBIEClient init begin...");
            ipAddr = ConfigUtil.getConfig("host");
            fpPort = Integer.parseInt(ConfigUtil.getConfig("fpPort"));
            ppPort = Integer.parseInt(ConfigUtil.getConfig("ppPort"));
            lppPort = Integer.parseInt(ConfigUtil.getConfig("lppPort"));
            plpPort = Integer.parseInt(ConfigUtil.getConfig("plpPort"));
            facePort = Integer.parseInt(ConfigUtil.getConfig("facePort"));
            irisPort = Integer.parseInt(ConfigUtil.getConfig("irisPort"));

            hbie_FP = new HBIEClient<HSFPTenFp.Record>(ipAddr, fpPort);
            hbie_PP = new HBIEClient<HSFPFourPalm.Record>(ipAddr, ppPort);
            hbie_LPP = new HBIEClient<HSFPLatFp.Record>(ipAddr, lppPort);
            hbie_PLP = new HBIEClient<HSFPLatPalm.Record>(ipAddr, plpPort);
            hbie_FACE = new HBIEClient<THIDFace.Record>(ipAddr, facePort);
            hbie_IRIS = new HBIEClient<HSIris.Record>(ipAddr, irisPort);
            log.info("HBIEClient init end...");
        } catch (RemoteException e) {
            
        } catch (NotBoundException e) {

        } catch (MalformedURLException e) {

        }
    }
}
