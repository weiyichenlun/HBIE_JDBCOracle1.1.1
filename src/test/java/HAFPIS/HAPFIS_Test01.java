package HAFPIS;

import HAFPIS.Utils.DateUtil;
import HAFPIS.Utils.HbieUtil;
import HAFPIS.domain.SrchDataRec;
import com.hisign.bie.MatcherException;
import com.hisign.bie.hsfp.HSFPFourPalm;
import com.hisign.bie.hsfp.HSFPLatPalm;
import com.hisign.bie.hsfp.HSFPTenFp;
import com.hisign.bie.iris.HSIris;
import com.hisign.bie.thid.THIDFace;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.stream.FileImageInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by ZP on 2017/5/22.
 */
public class HAPFIS_Test01 {
    private final static Logger log = LoggerFactory.getLogger(HAPFIS_Test01.class);
    private QueryRunner qr;
    @Before
    public void before() {
//        qr = QueryRunnerUtil.getInstance();
    }

    @Test
    public void test1() {
    }

    @Test
    public void test() throws UnsupportedEncodingException {
        String path = "D:\\Resource\\data\\palm\\图片";
        int tasktype = 3;
        int datatype = 5;
        try {
            byte[] srchdata = getSrchData(path, datatype);
            for (int i = 0; i < srchdata.length; i++) {
                System.out.printf("%5d", srchdata[i]);
            }
            boolean is = insert_srch_table(srchdata, tasktype, datatype);
            System.out.println("insert " + (is ? "successful" : "failed"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test01() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String pid = name.split("@")[0];
        System.out.println(name);
        System.out.println(pid);
    }

    public boolean insert_srch_table(byte[] srchdata, int tasktype, int datatype) {
        String url = "INSERT INTO HAFPIS_SRCH_TASK (TASKIDD, TRANSNO, PROBEID, DATATYPE, TASKTYPE, STATUS, SRCHDATA, BEGTIME) VALUES(?,?,?,?,?,?,?,?)";
        String taskidd = UUID.randomUUID().toString().replace("-", "");
        String transno = UUID.randomUUID().toString().replace("-", "");
        String probeid = UUID.randomUUID().toString().replace("-", "");
        int status = 3;
        List<Object> param = new ArrayList<>();
        param.add(taskidd);
        param.add(transno);
        param.add(probeid);
        param.add(datatype);
        param.add(tasktype);
        param.add(status);
        param.add(srchdata);
        param.add(DateUtil.getFormatDate(System.currentTimeMillis()));
        try{
            return qr.update(url, param.toArray()) > 0;
        } catch (SQLException e) {
            log.error("SQLException: ", e);
            return false;
        }
    }


    public static synchronized byte[] getSrchData(String path, int type) throws IOException {
        SrchDataRec srchDataRec = new SrchDataRec();
        srchDataRec.probeId = (UUID.randomUUID().toString().replace("-", "")+" ").getBytes();
        System.out.println(new String(srchDataRec.probeId));
        try {
            switch (type) {
                case 1:
                    byte[][] features_tenfp = getFeatures_tenfp(path);
                    int[] len = new int[10];
                    for (int i = 0; i < features_tenfp.length; i++) {
                        if (features_tenfp[i] != null) {
                            len[i] = features_tenfp[i].length;
                        }
                    }
                    srchDataRec.rpmnt = features_tenfp;
                    srchDataRec.RpMntLen = len;
                    break;
                case 4:
                    byte[] feature_latfp = getFeature_latfp(path);
                    if (feature_latfp != null) {
                        srchDataRec.latfpmnt = feature_latfp;
                        srchDataRec.RpMntLen[0] = feature_latfp.length;
                    }
                    break;
                case 2:
                    byte[][] feature_palm = getFeatures_palm(path);
                    int[] len1 = new int[10];
                    for (int i = 0; i < feature_palm.length; i++) {
                        if (feature_palm[i] != null) {
                            len1[i] = feature_palm[i].length;
                        }
                    }
                    srchDataRec.palmmnt = feature_palm;
                    srchDataRec.PalmMntLen = len1;
                    break;
                case 5:
                    byte[] feature_latpalm = getFeature_latpalm(path);
                    if (feature_latpalm != null) {
                        srchDataRec.latpalmmnt = feature_latpalm;
                        srchDataRec.PalmMntLen[0] = feature_latpalm.length;
                    }
                    break;
                case 6:
                    byte[] feature_face = getFeature_face(path);
                    if (feature_face != null) {
                        srchDataRec.facemnt[0] = feature_face;
                        srchDataRec.FaceMntLen[0] = feature_face.length;
                    }
                    break;
                case 7:
                    byte[][] feature_iris = getFeature_iris(path);
                    int[] len_iris = new int[2];
                    for (int i = 0; i < feature_iris.length; i++) {
                        if (feature_iris[i] != null) {
                            len_iris[i] = feature_iris[i].length;
                        }
                    }
                    srchDataRec.irismnt = feature_iris;
                    srchDataRec.IrisMntLen = len_iris;
                    break;
            }
        } catch (RemoteException | MatcherException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ObjectOutputStream oos = new ObjectOutputStream(dos);
        srchDataRec.writeExternal(oos);
        byte[] temp = new byte[1024];
        ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        ObjectInputStream ois = new ObjectInputStream(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        int len = 0;
        while ((len = ois.read(temp) )!= -1) {
            baos1.write(temp, 0 , len);
        }
        baos1.flush();
        System.out.println("baos1 size is " + baos1.size());
        return baos1.toByteArray();
    }

    public static byte[] getFeature_face(String path) throws RemoteException, MatcherException {
        byte[] feature = new byte[0];
        File pathFile = new File(path);
        String[] files = pathFile.list();
        byte[] data = new byte[0];
        if (files[0].endsWith("JPG") || files[0].endsWith("jpg")) {
            data = read(path, files[0]);
            if (data != null) {
                THIDFace.ExtractFeature extractFeature = new THIDFace.ExtractFeature();
                extractFeature.image = data;
                THIDFace.ExtractFeature.Result result = HbieUtil.getInstance().hbie_FACE.process(extractFeature);
                feature = result.feature;
            }
        }
        return feature;
    }

    public static byte[][] getFeature_iris(String path) throws RemoteException, MatcherException {
        byte[][] features = new byte[2][];
        File pathFile = new File(path);
        String[] files = pathFile.list();
        byte[] data = new byte[0];
        HSIris.ExtractFeature extractFeature = new HSIris.ExtractFeature();
        if (files != null) {
            for (int i=0; i<files.length; i++) {
                extractFeature.image = read(path, files[i]);
                HSIris.ExtractFeature.Result result = HbieUtil.getInstance().hbie_IRIS.process(extractFeature);
                features[i] = result.feature;
            }
        }
        byte[] temp = features[0];
        features[0] = features[1];
        features[1] = temp;
        return features;
    }

    public static byte[][] getFeatures_palm(String path) throws RemoteException, MatcherException {
        byte[][] features = new byte[10][];
        File pathFile = new File(path);
        String[] files = pathFile.list();
        HSFPFourPalm.ExtractFeature extractFeature = new HSFPFourPalm.ExtractFeature();
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                extractFeature.images[i] = read(path, files[i]);
            }
            HSFPFourPalm.ExtractFeature.Result result = HbieUtil.getInstance().hbie_PP.process(extractFeature);
            features[0] = result.features[0];
            features[5] = result.features[1];
            features[4] = result.features[2];
            features[9] = result.features[3];
        }
        return features;
    }

    public static byte[] getFeature_latpalm(String path) throws RemoteException, MatcherException {
        byte[] feature = new byte[0];
        File pathFile = new File(path);
        String[] files = pathFile.list();
        byte[] data = new byte[0];
        if (files[0].endsWith("JPG") || files[0].endsWith("jpg")) {
            data = read(path, files[0]);
            if (data != null) {
                HSFPLatPalm.ExtractFeature extractFeature = new HSFPLatPalm.ExtractFeature();
                extractFeature.image = data;
                HSFPLatPalm.ExtractFeature.Result result = HbieUtil.getInstance().hbie_LPP.process(extractFeature);
                feature = result.feature;
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
        }
        for (int i = 0; i < map.size(); i++) {
            Future<byte[]> f = map.get(i);
            if (f != null) {
                byte[] fea = f.get();
                for (int j = 0; j <fea.length; j++) {
                    System.out.printf("%5d",fea[j]);
                }
                System.out.println();
                features[i] = f.get();
            } else {
                features[i] = null;
            }
        }
        return features;
    }



    public static byte[] getFeature(byte[] image, int type) {
        byte[] feature = new byte[0];
        if (image != null) {
            try {
                switch (type) {
                    case 1:
                    case 4:
                        HSFPTenFp.ExtractFeature extractFeature = new HSFPTenFp.ExtractFeature();
                        extractFeature.image = image;
                        HSFPTenFp.ExtractFeature.Result result = new HSFPTenFp.ExtractFeature.Result();
                        result = HbieUtil.getInstance().hbie_FP.process(extractFeature);
                        feature = result.feature;
                        break;
                    case 2:
                    case 5:
                        HSFPFourPalm.ExtractFeature extractFeature1 = new HSFPFourPalm.ExtractFeature();
                        extractFeature1.images[0] = image;
                        HSFPFourPalm.ExtractFeature.Result result1 = new HSFPFourPalm.ExtractFeature.Result();
                        result1 = HbieUtil.getInstance().hbie_PP.process(extractFeature1);
                        feature = result1.features[0];
                        break;
                    case 6:
                        THIDFace.ExtractFeature extractFeature2 = new THIDFace.ExtractFeature();
                        extractFeature2.image = image;
                        THIDFace.ExtractFeature.Result result2 = new THIDFace.ExtractFeature.Result();
                        result2 = HbieUtil.getInstance().hbie_FACE.process(extractFeature2);
                        feature = result2.feature;
                        break;
                    case 7:
                        HSIris.ExtractFeature extractFeature3 = new HSIris.ExtractFeature();
                        extractFeature3.image = image;
                        HSIris.ExtractFeature.Result result3 = new HSIris.ExtractFeature.Result();
                        result3 = HbieUtil.getInstance().hbie_IRIS.process(extractFeature3);
                        feature = result3.feature;
                        break;
                    default:
                        log.error("type error: {}", type);
                        break;
                }
            } catch (RemoteException e) {
                e.printStackTrace();
            } catch (MatcherException e) {
                e.printStackTrace();
            }
        } else {
            log.error("image is null.");
            feature = null;
        }
        return feature;
    }


    public static byte[] read(File temp) {
        byte[] data = null;
        FileImageInputStream input = null;
        try {
            input = new FileImageInputStream(temp);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buff = new byte[1024];
            int numBytesRead = 0;
            while ((numBytesRead = input.read(buff)) != -1) {
                output.write(buff, 0, numBytesRead);
            }
            data = output.toByteArray();
            output.close();
            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    public static synchronized byte[] read(String path, String name) {
        File file = new File(path, name);
        return read(file);
    }

}
