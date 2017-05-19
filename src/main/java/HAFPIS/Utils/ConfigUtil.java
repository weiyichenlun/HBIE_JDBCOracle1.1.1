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
 * 对hbie.cfg.properties进行缓存，如果文件发生了更改则重新进行加载，否则直接
 * 从内存里面读取相应信息
 * Created by ZP on 2017/5/12.
 */
public class ConfigUtil {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtil.class);
    private static Properties props = null;
    private static File configFile = null;
    private static long fileLastModified = 0;
    private static final String ERROR_MESSAGE = "ERROR";
    private static String configFileName = "config\\hbie.cfg.properties";

    private static synchronized void init() {
        configFile = new File(configFileName);
        log.info("configfile abs path is {}", configFile.getAbsolutePath());
        fileLastModified = configFile.lastModified();
        props = new Properties();
        load();
    }

    private static synchronized void load() {
        try{
            props.load(new FileInputStream(configFileName));
            fileLastModified = configFile.lastModified();
        } catch (FileNotFoundException e) {
            log.error("can not load configFile {}.", configFileName, e);
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
        URL url = ConfigUtil.class.getResource(configFileName);
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
        URL url = ConfigUtil.class.getResource(configFileName);
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
        URL url = ConfigUtil.class.getResource(configFileName);
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

    public static synchronized Properties getProp(String[] args) {
        Properties prop = new Properties();
        for (int i = 0; i < args.length; i++) {
            String name = args[i];
            log.info("name is {}", name);
            if (name.startsWith("-")) {
                if (name.startsWith("-cfg-file=")) {
                    String temp = name.substring(name.indexOf(61) + 1);
                    InputStream is = null;
                    try {
                        log.info("file name is {}", temp);
                        File t = new File(temp);
                        log.info("after file, the abs path is {}", t.getAbsolutePath());
                        log.info("after file, the canonical path is {}", t.getCanonicalPath());
                        is = new FileInputStream(temp);
                        prop.load(is);
                        is.close();
                    } catch (IOException e) {
                        log.error("load file error: {}, exception: {}", temp, e);
                    } finally {
                        if (is != null) {
                            try {
                                is.close();
                            } catch (IOException e) {
                            }
                        }
                    }
                } else {
                    int t = name.indexOf(61);
                    if (t == -1) {
                        prop.setProperty(name.substring(1), "true");
                    } else {
                        prop.setProperty(name.substring(1, t), name.substring(t+1));
                    }
                }

            }
        }
        return prop;
    }

    public static synchronized Properties getProp(String configFileName) {
            File config = new File(configFileName);
            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(config));
            } catch (IOException e) {
                log.error("can not load configFile {}.", configFileName, e);
            }
            return properties;
    }

    @Test
    public void test() {
        System.out.println(ConfigUtil.class.getResource(""));
        System.out.println(ConfigUtil.class.getResource("/"));
        System.out.println(ConfigUtil.class.getClassLoader().getResource(""));
    }


}
