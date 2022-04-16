package com.lby.flink.utils;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author ：lby
 * @date ：Created at 2022/1/3 21:11
 * @desc ：获取properties工具类
 */
public class PropertiesUtils {
    public static String getDBProperty(String key) throws IOException {
        InputStream in = PropertiesUtils.class.getClassLoader().getResourceAsStream("db.properties");
        Properties prop = new Properties();
        prop.load(in);
        return prop.getProperty(key);
    }
}
