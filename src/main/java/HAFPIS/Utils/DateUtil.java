package HAFPIS.Utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 时间处理工具类
 * Created by ZP on 2017/5/15.
 */
public class DateUtil {
    public static String getFormatDate(long time_end) {
        Date adate = new Date(time_end);
        return getFormatDate("yyyy-MM-dd HH:mm:ss", adate);
    }

    public static String getFormatDate(Date str){
        return getFormatDate("yyyy-MM-dd HH:mm:ss",str);
    }

    public static String getFormatDate(String formatString, Date adate) {
        String currentDate="";
        SimpleDateFormat format = new SimpleDateFormat(formatString);
        currentDate = format.format(adate);
        return currentDate;
    }

    public static String getFormatDate(String formatString, String str){
        String currentDate = "";
        SimpleDateFormat format1 = new SimpleDateFormat(formatString);
        currentDate = format1.format(str);
        return currentDate;
    }


}
