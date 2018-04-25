package HAFPIS;

import HAFPIS.Utils.HbieUtil;
import HAFPIS.Utils.QueryRunnerUtil;
import com.hisign.bie.MatcherException;
import com.hisign.bie.hsfp.HSFPFourPalm;
import com.hisign.bie.hsfp.HSFPLatPalm;
import com.hisign.bie.hsfp.HSFPTenFp;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.imageio.stream.FileImageInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** 
* HAFPIS_Main Tester. 
* 
* @author <Authors name> 
* @since <pre>���� 12, 2017</pre> 
* @version 1.0 
*/ 
public class HAFPIS_MainTest { 
    private static QueryRunner qr;
    @Before
    public void before() throws Exception {
        qr = QueryRunnerUtil.getInstance();
    }

    @Test
    public void test_insert_palm_feature() throws RemoteException, MatcherException {
        String path = "D:\\Resource\\data\\palm\\image\\test1";
        byte[][] features = getFeatures_palm(path);
        insert_PMMNT(features);
    }

    private static void insert_PMMNT(byte[][] features) {

    }

    @Test
    public void test_insert_tenfp_srch() throws MatcherException, IOException, SQLException, ExecutionException, InterruptedException {
        insertSrchTask_tenfp("D:\\Resource\\data\\gallery\\R4403035000002011020288_1", "1");
    }

    @Test
    public void test_insert_latfp_srch() throws IOException, SQLException, MatcherException {
//        insertSrchTask_latfp("D:\\Resource\\data\\gallery\\R4403035000002011020288_1");
    }

    @Test
    public void test_insert_fourpalm_srch() throws IOException, MatcherException, SQLException {
        String path = "D:\\Resource\\data\\palm\\image\\test1";
        byte[] srchdata = getSrchData_Palm(path);

        insertSrchTask_fourpalm(srchdata, 1);
    }
    @Test
    public void test_insert_1ToFTT_srch() throws IOException, MatcherException, SQLException, ExecutionException, InterruptedException {
        String path = "D:\\Resource\\data\\gallery";
        File pathFile = new File(path);
        File[] paths = pathFile.listFiles();
        String[] res = new String[5];
        int idx = 0;
        for (int i = 0; i <paths.length; i++) {
            if (paths[i].isDirectory() && idx < 5) {
                if(paths[i].listFiles().length>=10) {
                    res[idx++] = paths[i].getAbsolutePath();
                }
            }
        }
        byte[] srchdata = getSrchDatas_TenFP(res);
        insertSrchTask_tenfp_1ToF(srchdata, 8);

    }

    @Test
    public void test_insert_1ToFLL_srch() throws IOException, MatcherException, SQLException {
        String path = "D:\\Resource\\data\\gallery\\";
        File pathFile = new File(path);
        File[] paths = pathFile.listFiles();
        String[] res = new String[10];
        int idx = 0;
        for (int i = 0; i < paths.length; i++) {
            if (paths[i].isDirectory() && idx < 10) {
                res[idx++] = paths[i].getAbsolutePath();
            }
        }
        byte[] srchdata = getSrchDatas_LatFp(res);
        insertSrchTask_latfp_1ToF(srchdata, 8);
    }

    @Test
    public void test_insert_1ToF_PPTT_srch() throws IOException, MatcherException, SQLException {
        String path = "D:\\Resource\\data\\palm\\image";
        File pathFile = new File(path);
        File[] paths = pathFile.listFiles();
        assert paths != null;
        int idx = 0;
        String[] res = new String[paths.length];
        for (int i = 0; i < paths.length; i++) {
            if (paths[i].isDirectory()) {
                res[idx++] = paths[i].getAbsolutePath();
            }
        }
        byte[] srchdata = getSrchDatas_palm(res);
        insertSrchTask_fourpalm_1ToF(srchdata, 8);

    }

    @Test
    public void test_insert_1ToF_PPLL_srch() throws SQLException, IOException, MatcherException {
        String path = "D:\\Resource\\data\\palm\\lat_image";
        File pathFile = new File(path);
        File[] paths = pathFile.listFiles();
        assert paths != null;
        int idx = 0;
        String[] res = new String[paths.length];
        for (int i = 0; i < paths.length; i++) {
            if (paths[i].isDirectory()) {
                res[idx++] = paths[i].getAbsolutePath();
            }
        }
        byte[] srchdata = getSrchDatas_latpalm(res);
        insertSrchTask_latpalm_1ToF(srchdata, 8);
    }

    @After
    public void after() throws Exception {
    }

    public static void insertSrchTask_latfp(String path) throws SQLException, IOException, MatcherException {
        String url = "INSERT INTO HAFPIS_SRCH_TASK (TASKIDD, TRANSNO, PROBEID, DATATYPE, TASKTYPE, STATUS, SRCHDATA) VALUES(?,?,?,?,?,?,?)";
        String taskidd = "0000002";
        String transno = "0000002";
        String probeid = "00000000000000000000000000000002";
        int datatype = 4;
        int tasktype = 3;
        int status = 3;
        byte[] srchdata = getSrchData_LatFp(path);
        System.out.println("srchdata length is "+srchdata.length);
        int a = qr.update(url,taskidd,transno, probeid,datatype,tasktype,status,srchdata);
        System.out.println(a);
    }

    public static byte[] getSrchDatas_palm(String[] paths) throws IOException, MatcherException {
        int len = paths.length;
        //TODO 假设所有特征都不为空
        byte[] res = new byte[(40960 + 353) * len];
        for (int i = 0; i < len; i++) {
            String path = paths[i];
            System.out.println(path);
            byte[] srchdata = getSrchData_Palm(path);
            System.out.println("srchdata length is " + srchdata.length);
            System.arraycopy(srchdata, 0, res, (40960 + 353) * i, (40960 + 353));
        }
        return res;
    }

    public static byte[] getSrchDatas_latpalm(String[] paths) throws IOException, MatcherException {
        int len = paths.length;
        byte[] res = new byte[(8192 + 353) * len];
        for (int i = 0; i < len; i++) {
            String path = paths[i];
            byte[] srchdata = getSrchData_latpalm(path);
            System.arraycopy(srchdata, 0, res, (8192 + 353) * i, (8192 + 353));
        }
        return res;
    }

    public static byte[] getSrchDatas_LatFp(String[] paths) throws IOException, MatcherException {
        int len = paths.length;
        byte[] res = new byte[(3072 + 353) * len];
        for (int i = 0; i < len; i++) {
            String path = paths[i];
            byte[] srchdata = getSrchData_LatFp(path);
            System.arraycopy(srchdata, 0, res, (3072 + 353) * i, (3072 + 353));
        }
        return res;
    }

    public static byte[] getSrchDatas_TenFP(String[] paths) throws IOException, MatcherException, ExecutionException, InterruptedException {
        int len = paths.length;
        byte[] res = new byte[31073 * len];
        for (int i = 0; i < len; i++) {
            String path = paths[i];
            System.out.println("in getSrchdatas_tenfp path is " + path);
            byte[] srchdata = getSrchData_TenFp(path);
            System.arraycopy(srchdata, 0, res, 31073 * i, 31073);
        }
        return res;

    }

    public static void insertSrchTask_fourpalm_1ToF(byte[] srchdata, int tasktype) throws SQLException {
        String url = "INSERT INTO HAFPIS_SRCH_TASK (TASKIDD, TRANSNO, PROBEID, DATATYPE, TASKTYPE, STATUS, SRCHDATA) VALUES(?,?,?,?,?,?,?)";
        String taskidd = UUID.randomUUID().toString().replace("-", "");
        String transno = UUID.randomUUID().toString().replace("-", "");
        byte[] id = new byte[32];
        for (int i = 0; i < 32; i++) {
            id[i] = srchdata[i];
        }
        String probeid = new String(id);
        int datatype = 2;
        int status = 3;
        System.out.println("srchdata length is "+srchdata.length);
        int a = qr.update(url,taskidd,transno, probeid,datatype,tasktype,status,srchdata);
        System.out.println(a);
    }

    public static void insertSrchTask_latpalm_1ToF(byte[] srchdata, int tasktype) throws SQLException {
        String url = "INSERT INTO HAFPIS_SRCH_TASK (TASKIDD, TRANSNO, PROBEID, DATATYPE, TASKTYPE, STATUS, SRCHDATA) VALUES(?,?,?,?,?,?,?)";
        String taskidd = UUID.randomUUID().toString().replace("-", "");
        String transno = UUID.randomUUID().toString().replace("-", "");
        byte[] id = new byte[32];
        for (int i = 0; i < 32; i++) {
            id[i] = srchdata[i];
        }
        String probeid = new String(id);
        int datatype = 5;
        int status = 3;
        System.out.println("srchdata length is "+srchdata.length);
        int a = qr.update(url,taskidd,transno, probeid,datatype,tasktype,status,srchdata);
        System.out.println(a);
    }

    public static void insertSrchTask_latfp_1ToF(byte[] srchdata, int tasktype) throws SQLException {
        String url = "INSERT INTO HAFPIS_SRCH_TASK (TASKIDD, TRANSNO, PROBEID, DATATYPE, TASKTYPE, STATUS, SRCHDATA) VALUES(?,?,?,?,?,?,?)";
        String taskidd = UUID.randomUUID().toString().replace("-", "");
        String transno = UUID.randomUUID().toString().replace("-", "");
        byte[] id = new byte[32];
        for (int i = 0; i < 32; i++) {
            id[i] = srchdata[i];
        }
        String probeid = new String(id);
        int datatype = 4;
        int status = 3;
        System.out.println("srchdata length is "+srchdata.length);
        int a = qr.update(url,taskidd,transno, probeid,datatype,tasktype,status,srchdata);
        System.out.println(a);
    }

    public static void insertSrchTask_fourpalm(byte[] srchdata, int tasktype) throws SQLException {
        String url = "INSERT INTO HAFPIS_SRCH_TASK (TASKIDD, TRANSNO, PROBEID, DATATYPE, TASKTYPE, STATUS, SRCHDATA) VALUES(?,?,?,?,?,?,?)";
        String taskidd = UUID.randomUUID().toString().replace("-", "");
        String transno = UUID.randomUUID().toString().replace("-", "");
        byte[] id = new byte[32];
        for (int i = 0; i < 32; i++) {
            id[i] = srchdata[i];
        }
        String probeid = new String(id);
        int datatype = 2;
        int status = 3;
        System.out.println("srchdata length is "+srchdata.length);
        int a = qr.update(url,taskidd,transno, probeid,datatype,tasktype,status,srchdata);
        System.out.println(a);
    }

    public static void insertSrchTask_tenfp_1ToF(byte[] srchdata, int tasktype) throws SQLException {
        String url = "INSERT INTO HAFPIS_SRCH_TASK (TASKIDD, TRANSNO, PROBEID, DATATYPE, TASKTYPE, STATUS, SRCHDATA) VALUES(?,?,?,?,?,?,?)";
        String taskidd = UUID.randomUUID().toString().replace("-", "");
        String transno = UUID.randomUUID().toString().replace("-", "");
        byte[] id = new byte[32];
        for (int i = 0; i < 32; i++) {
            id[i] = srchdata[i];
        }
        String probeid = new String(id);
        int datatype = 1;
        int status = 3;
        System.out.println("srchdata length is "+srchdata.length);
        int a = qr.update(url,taskidd,transno, probeid,datatype,tasktype,status,srchdata);
        System.out.println(a);
    }

    public static void insertSrchTask_tenfp(String path, String tasktype) throws IOException, MatcherException, SQLException, ExecutionException, InterruptedException {
        String url = "INSERT INTO HAFPIS_SRCH_TASK (TASKIDD, TRANSNO, PROBEID, DATATYPE, TASKTYPE, STATUS, SRCHDATA) VALUES(?,?,?,?,?,?,?)";
        String taskidd = UUID.randomUUID().toString().replace("-", "");
        String transno = UUID.randomUUID().toString().replace("-", "");
        String probeid = UUID.randomUUID().toString().replace("-", "");
        int datatype = 1;
        int status = 3;
        byte[] srchdata = getSrchData_TenFp(path);
        System.out.println("srchdata length is "+srchdata.length);
        int a = qr.update(url,taskidd,transno, probeid,datatype,tasktype,status,srchdata);
        System.out.println(a);
    }

    public static byte[] getSrchData_Palm(String path) throws IOException, MatcherException {
        byte[][] features = getFeatures_palm(path);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        byte[] temp = new byte[33];
        temp = (UUID.randomUUID().toString().replace("-", "")+" ").getBytes();
        dos.write(temp);
        int[] rollMnt = new int[10];
        int[] rollimg = new int[10];
        int[] faltMnt = new int[10];
        int[] faltimg = new int[10];
        int[] palmMnt = new int[10];
        int[] palmimg = new int[10];
        int[] faceMnt = new int[3];
        int[] faceimg = new int[3];
        int[] irisMnt = new int[2];
        int[] irisimg = new int[2];
        int[] reserved = new int[10];
        for (int i : rollMnt) {
            dos.writeInt(i);
        }
        for (int i : rollimg) {
            dos.writeInt(i);
        }
        for (int i : faltMnt) {
            dos.writeInt(i);
        }
        for (int i : faltimg) {
            dos.writeInt(i);
        }
        for (int i=0; i<palmMnt.length; i++) {
            if (0 == i && null != features[0]) {
                System.out.println("0" + features[0].length);
                dos.writeInt(features[0].length);
            } else if (4 == i && null != features[2]) {
                System.out.println("2" + features[2].length);
                dos.writeInt(features[2].length);
            } else if (5 == i && null != features[1]) {
                System.out.println("1" + features[1].length);
                dos.writeInt(features[1].length);
            } else if (9 == i && null != features[3]) {
                System.out.println("3" + features[3].length);
                dos.writeInt(features[3].length);
            } else {
                dos.writeInt(0);
            }
        }
        for (int i : palmimg) {
            dos.writeInt(i);
        }
        for (int i : faceMnt) {
            dos.writeInt(i);
        }
        for (int i : faceimg) {
            dos.writeInt(i);
        }
        for (int i : irisMnt) {
            dos.writeInt(i);
        }
        for (int i : irisimg) {
            dos.writeInt(i);
        }
        for (int i:reserved) {
            dos.writeInt(i);
        }

        dos.flush();
        dos.write(features[0]);
        dos.write(features[2]);
        dos.write(features[1]);
        dos.write(features[3]);
        dos.flush();
        return baos.toByteArray();

    }

    public static byte[] getSrchData_latpalm(String path) throws IOException, MatcherException {
        byte[] feature = new byte[0];
        feature = getFeature_latpalm(path);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        byte[] temp = new byte[33];
        temp = (UUID.randomUUID().toString().replace("-", "")+" ").getBytes();
        dos.write(temp);
        int[] rollMnt = new int[10];
        int[] rollimg = new int[10];
        int[] faltMnt = new int[10];
        int[] faltimg = new int[10];
        int[] palmMnt = new int[10];
        int[] palmimg = new int[10];
        int[] faceMnt = new int[3];
        int[] faceimg = new int[3];
        int[] irisMnt = new int[2];
        int[] irisimg = new int[2];
        int[] reserved = new int[10];
        palmMnt[0] = feature.length;
        for (int i : rollMnt) {
            dos.writeInt(i);
        }
        for (int i : rollimg) {
            dos.writeInt(i);
        }
        for (int i : faltMnt) {
            dos.writeInt(i);
        }
        for (int i : faltimg) {
            dos.writeInt(i);
        }
        for (int i : palmMnt) {
            dos.writeInt(i);
        }
        for (int i : palmimg) {
            dos.writeInt(i);
        }
        for (int i : faceMnt) {
            dos.writeInt(i);
        }
        for (int i : faceimg) {
            dos.writeInt(i);
        }
        for (int i : irisMnt) {
            dos.writeInt(i);
        }
        for (int i : irisimg) {
            dos.writeInt(i);
        }
        for (int i:reserved) {
            dos.writeInt(i);
        }

        dos.flush();
        dos.write(feature);
        dos.flush();
        return baos.toByteArray();
    }

    public static byte[] getSrchData_LatFp(String path) throws IOException, MatcherException {
        byte[] feature = new byte[0];
        feature = getFeature_latfp(path);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        byte[] temp = new byte[33];
        temp = (UUID.randomUUID().toString().replace("-", "")+" ").getBytes();
        dos.write(temp);
        int[] rollMnt = new int[10];
        int[] rollimg = new int[10];
        int[] faltMnt = new int[10];
        int[] faltimg = new int[10];
        int[] palmMnt = new int[10];
        int[] palmimg = new int[10];
        int[] faceMnt = new int[3];
        int[] faceimg = new int[3];
        int[] irisMnt = new int[2];
        int[] irisimg = new int[2];
        int[] reserved = new int[10];
        rollMnt[0] = feature.length;
        for (int i : rollMnt) {
            dos.writeInt(i);
        }
        for (int i : rollimg) {
            dos.writeInt(i);
        }
        for (int i : faltMnt) {
            dos.writeInt(i);
        }
        for (int i : faltimg) {
            dos.writeInt(i);
        }
        for (int i : palmMnt) {
            dos.writeInt(i);
        }
        for (int i : palmimg) {
            dos.writeInt(i);
        }
        for (int i : faceMnt) {
            dos.writeInt(i);
        }
        for (int i : faceimg) {
            dos.writeInt(i);
        }
        for (int i : irisMnt) {
            dos.writeInt(i);
        }
        for (int i : irisimg) {
            dos.writeInt(i);
        }
        for (int i:reserved) {
            dos.writeInt(i);
        }

        dos.flush();
        dos.write(feature);
        dos.flush();
        return baos.toByteArray();
    }

    public static byte[] getSrchData_TenFp(String path) throws IOException, MatcherException, ExecutionException, InterruptedException {
        byte[] srchData = new byte[0];
        //D:\Resource\data\gallery\R4403035000002011020288_1\R4403035000002011020288_1_01.bmp
        byte[][] features = new byte[10][];
        features = getFeatures_tenfp(path);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        char[] probeid = new char[32];
        byte[] temp = new byte[33];
        temp = (UUID.randomUUID().toString().replace("-", "")+" ").getBytes();
        dos.write(temp);
        int[] rollMnt = new int[10];
        int[] rollimg = new int[10];
        int[] faltMnt = new int[10];
        int[] faltimg = new int[10];
        int[] palmMnt = new int[10];
        int[] palmimg = new int[10];
        int[] faceMnt = new int[3];
        int[] faceimg = new int[3];
        int[] irisMnt = new int[2];
        int[] irisimg = new int[2];
        int[] reserved = new int[10];
        for (int i = 0; i < rollMnt.length; i++) {
            if (features[i] != null && features[i].length > 0) {
                System.out.println(features[i].length);
                dos.writeInt(features[i].length);
            } else {
                dos.writeInt(0);
            }
        }
        for(int i:rollimg){
            dos.writeInt(i);
        }
        for(int i:faltMnt){
            dos.writeInt(i);
        }
        for(int i:faltimg){
            dos.writeInt(i);
        }
        for(int i:palmMnt) {
            dos.writeInt(i);
        }
        for(int i:palmimg){
            dos.writeInt(i);
        }
        for(int i:faceMnt) {
            dos.writeInt(i);
        }
        for(int i:faceimg){
            dos.writeInt(i);
        }
        for(int i:irisMnt){
            dos.writeInt(i);
        }
        for(int i:irisimg){
            dos.writeInt(i);
        }
        for (int i:reserved) {
            dos.writeInt(i);
        }

        dos.flush();
        for (int i = 0; i < 10; i++) {
            if (features[i] != null) {
                dos.write(features[i]);
            }
        }
        dos.flush();
        return baos.toByteArray();
    }

    public static byte[] getFeature_latpalm(String path) throws RemoteException, MatcherException {
        byte[] feature = new byte[0];
        File pathFile = new File(path);
        String[] files = pathFile.list();
        byte[] data = new byte[0];
        if (files[0].toLowerCase().endsWith("jpg") || files[0].toLowerCase().endsWith("bmp")) {
            data = read(path, files[0]);
            if (data != null) {
                System.out.println("data is not null");
                HSFPLatPalm.ExtractFeature extractFeature = new HSFPLatPalm.ExtractFeature();
                extractFeature.image = data;
                HSFPLatPalm.ExtractFeature.Result result = HbieUtil.getInstance().hbie_PLP.process(extractFeature);
                feature = result.feature;
                System.out.println("feature length is " + feature.length);
            }
        }
        return feature;
    }

    public static byte[] getFeature_latfp(String path) throws RemoteException, MatcherException {
        byte[] feature = new byte[0];
        File pathFile = new File(path);
        String[] files = pathFile.list();
        byte[] data = new byte[0];
        if (files[0].endsWith("bmp")) {
            data = read(path, files[0]);
            if (data != null) {
                HSFPTenFp.ExtractFeature extractFeature = new HSFPTenFp.ExtractFeature();
                extractFeature.image = data;
                HSFPTenFp.ExtractFeature.Result result = HbieUtil.getInstance().hbie_FP.process(extractFeature);
                feature = result.feature;
            }
        }
        return feature;
    }

    public static byte[][] getFeatures_palm(String path) throws RemoteException, MatcherException {
        File pathFIle = new File(path);
        String[] files = pathFIle.list();
        assert files != null;
        byte[][] data = new byte[4][];
        for (int i = 0; i < files.length; i++) {
            if (files[i].toLowerCase().endsWith("jpg") || files[i].toLowerCase().endsWith("bmp")) {
                data[i] = read(path, files[i]);
            }
        }
        HSFPFourPalm.ExtractFeature extractFeature = new HSFPFourPalm.ExtractFeature();
        extractFeature.images = data;
        HSFPFourPalm.ExtractFeature.Result result = HbieUtil.getInstance().hbie_PP.process(extractFeature);
        return result.features;
    }

    public static byte[][] getFeatures_tenfp(String path) throws RemoteException, MatcherException, ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        byte[][] features = new byte[10][];
        File pathFile = new File(path);
        String[] files = pathFile.list();
        byte[] data = new byte[0];
        Map<Integer, Future<byte[]>> map = new HashMap<>();
        for (int i = 0; i < files.length; i++) {
            if (files[i].endsWith("bmp")) {
                data = read(path, files[i]);
                if (data != null) {
                    final byte[] finalData = data;
                    Future<byte[]> future = executorService.submit(new Callable<byte[]>() {
                        @Override
                        public byte[] call() throws Exception {
                            HSFPTenFp.ExtractFeature extract = new HSFPTenFp.ExtractFeature();
                            extract.image = finalData;
                            HSFPTenFp.ExtractFeature.Result result = HbieUtil.getInstance().hbie_FP.process(extract);
                            return result.feature;
                        }
                    });
                    map.put(i, future);
                }

            }

//            if (files[i].endsWith("bmp")) {
//                data = read(path, files[i]);
//                if (data != null) {
//                    HSFPTenFp.ExtractFeature extract = new HSFPTenFp.ExtractFeature();
//                    extract.image = data;
//                    HSFPTenFp.ExtractFeature.Result result = HbieUtil.getInstance().hbie_FP.process(extract);
//                    features[i] = result.feature;
//                }
//
//            }
        }
        for (int i = 0; i < map.size(); i++) {
            Future<byte[]> f = map.get(i);
            if (f != null) {
                features[i] = f.get();
            } else {
                features[i] = null;
            }
        }
        return features;
    }


    private static synchronized byte[] read(String path, String name) {
        byte[] data = null;
        FileImageInputStream input = null;
        try {
            input = new FileImageInputStream(new File(path, name));
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buff = new byte[1024];
            int numBytesRead = 0;
            while ((numBytesRead = input.read(buff)) != -1) {
                output.write(buff, 0, numBytesRead);
            }
            data = output.toByteArray();
            output.close();
            input.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }

    private static synchronized byte[] read(File file) {
        byte[] data = null;
        FileImageInputStream input = null;
        try {
            input = new FileImageInputStream(file);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buff = new byte[1024];
            int numBytesRead = 0;
            while ((numBytesRead = input.read(buff)) != -1) {
                output.write(buff, 0, numBytesRead);
            }
            data = output.toByteArray();
            output.close();
            input.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }

} 
