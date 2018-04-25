package HAFPIS;

import com.hisign.bie.DBException;
import com.hisign.bie.MatcherException;

import javax.imageio.stream.FileImageInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * 描述：
 * 作者：ZP
 * 创建时间:2018/2/1
 * 最后修改时间:2018/2/1
 */
public class HbieClientTest {
    public static void main(String[] args) throws RemoteException, NotBoundException, MalformedURLException, DBException, MatcherException {



//
//        HBIEClient<HSFPTenFp.Record> client = new HBIEClient<>("172.16.0.193", 1099);
//        HSFPTenFp.Record record = client.get("R4500002300002017120018$");
//        System.out.println(record.id + ", " + record.version);
////
//        String file = "D:\\HBIE_JDBCOracle1.1.3\\src\\test\\java\\HAFPIS\\1111.png";
//        byte[] image = read(new File(file));
//        HSFPTenFp.ExtractFeature extract = new HSFPTenFp.ExtractFeature();
//        extract.image = image;
//        HSFPTenFp.ExtractFeature.Result extractResult = client.process(extract);
//        byte[] fea = extractResult.feature;
//        System.out.println("feature length is " + fea.length);
//
//        HSFPTenFp.TenFpSearchParam tt = new HSFPTenFp.TenFpSearchParam();
//        tt.features[0] = fea;
////        tt.features = record.features;
//        tt.id = "111233";
//        tt.n = 10;
//        SearchResults<HSFPTenFp.TenFpSearchParam.Result> results = client.search(tt);
//        for (HSFPTenFp.TenFpSearchParam.Result result : results.candidates) {
//            System.out.println(result.record.id + ", " + result.score);
//        }

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
}
