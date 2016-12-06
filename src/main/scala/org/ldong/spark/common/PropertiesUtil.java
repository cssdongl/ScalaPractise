package org.ldong.spark.common;

import java.io.IOException;
import java.util.Properties;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2016/12/6 16:36
 */
public class PropertiesUtil {
    private static Properties p = new Properties();

    /**
     * 读取properties配置文件信息
     */
    static {
        try {
            p.load(PropertiesUtil.class.getClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据key得到value的值
     */
    public static String getValue(String key) {
        return p.getProperty(key);
    }

    public static void main(String[] args) {
        String address = PropertiesUtil.getValue("NAME_NODE_ADDRESS");
        System.out.println(address);
    }

}
