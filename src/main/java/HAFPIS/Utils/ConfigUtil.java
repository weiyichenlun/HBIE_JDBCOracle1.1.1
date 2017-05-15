package HAFPIS.Utils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

/**
 * 配置文件工具类
 * Created by ZP on 2017/5/12.
 */
public class ConfigUtil {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtil.class);
    private static Properties props = null;
    private static File configFile = null;
    private static long fileLastModified = 0;
    private static final String ERROR_MESSAGE = "ERROR";
    private static String configFileName = "hbie.cfg.properties";

    private static synchronized void init() {
        try {
            URL url = ConfigUtil.class.getClassLoader().getResource(configFileName);
            if (url == null) {
                log.error("get resource file {] error.", configFileName);
            }else{
                String path = URLDecoder.decode(url.getPath(), "UTF-8");
                configFile = new File(path);
                fileLastModified = configFile.lastModified();
                props = new Properties();
                load();
            }
        } catch (UnsupportedEncodingException e) {
            log.error("can not decode url path in UTF-8. ", e);
        }
    }

    private static synchronized void load() {
        try{
            props.load(new FileInputStream(configFile));
            fileLastModified = configFile.lastModified();
        } catch (FileNotFoundException e) {
            log.error("can not configFile {}.", configFileName, e);
        } catch (IOException e) {
            log.error("IOException while in loading configfile {}.",configFileName, e);
        }
    }
    public static synchronized String getConfig(String key) {
        if (configFile == null || props == null) {
            init();
        }
        if(configFile.lastModified() > fileLastModified ) load();
        return props.getProperty(key);
    }

    public static synchronized String getConfig(String configFileName, String key) {
        URL url = ConfigUtil.class.getClassLoader().getResource(configFileName);
        if (url == null) {
            log.error("get resource file {} error.", configFileName);
            return ERROR_MESSAGE;
        } else {
            String path = url.getPath();
            try {
                path = URLDecoder.decode(path, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                log.error("can not configFile {}.", configFileName, e);
            }
            File config = new File(path);
            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(config));
            } catch (IOException e) {
                log.error("can not load configFile {}.", configFileName, e);
            }
            return properties.getProperty(key);
        }
    }

    public static synchronized String writeConfig(String configFileName, String key, String value) {
        URL url = ConfigUtil.class.getClassLoader().getResource(configFileName);
        if (url == null) {
            log.error("get resource file {] error.", configFileName);
            return ERROR_MESSAGE;
        }
        String path = url.getPath();
        try {
            path = URLDecoder.decode(path, "UTF-8");
        } catch (UnsupportedEncodingException e1) {
            log.error("can not configFile {}.", configFileName, e1);
        }
        File config = new File(path);
        Properties properties = new Properties();
        String oldValue = null;
        try {
            InputStream is = new FileInputStream(config);
            properties.load(is);
            is.close();
            OutputStream out = new FileOutputStream(config);
            oldValue = (String) properties.get(key);
            properties.setProperty(key, value);
            properties.store(out, key);
            out.close();
        }catch (IOException e) {
            log.error("can not load configFile {}.", configFileName, e);
        }
        return oldValue;
    }

    public static synchronized String writeConfig(String key, String value) {
        URL url = ConfigUtil.class.getClassLoader().getResource(configFileName);
        if (url == null) {
            log.error("get resource file {} error.", configFileName);
            return ERROR_MESSAGE;
        } else {
            String path = url.getPath();
            try {
                path = URLDecoder.decode(path, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                log.error("can not configFile {}.", configFileName, e);
            }
            File config = new File(path);
            Properties props = new Properties();
            String oldValue = null;
            try {
                InputStream in = new FileInputStream(config);
                props.load(in);
                in.close();
                OutputStream out = new FileOutputStream(config);
                oldValue = (String) props.get(key);
                props.setProperty(key, value);
                props.store(out, key);
                out.close();
            } catch (IOException e) {
                log.error("can not load configFile {}.", configFileName, e);
            }
            return oldValue;
        }
    }

    public static synchronized Properties getProp(String configFileName) {
        URL url = ConfigUtil.class.getClassLoader().getResource(configFileName);
        if (url == null) {
            log.error("get resource file {} error.", configFileName);
            return null;
        } else {
            String path = url.getPath();
            try {
                path = URLDecoder.decode(path, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                log.error("can not configFile {}.", configFileName, e);
            }
            File config = new File(path);
            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(config));
            } catch (IOException e) {
                log.error("can not load configFile {}.", configFileName, e);
            }
            return properties;
        }
    }

    @Test
    public void test() {
        System.out.println(ConfigUtil.class.getResource(""));
        System.out.println(ConfigUtil.class.getResource("/"));
        System.out.println(ConfigUtil.class.getClassLoader().getResource(""));
    }

}
