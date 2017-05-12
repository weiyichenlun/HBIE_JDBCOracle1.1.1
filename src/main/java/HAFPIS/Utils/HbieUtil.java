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

import java.util.Properties;

/**
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
    private static Properties prop;

    static {
        try {
            log.info("HBIEClient init begin...");
        } catch (Exception e) {
        }
    }
}
