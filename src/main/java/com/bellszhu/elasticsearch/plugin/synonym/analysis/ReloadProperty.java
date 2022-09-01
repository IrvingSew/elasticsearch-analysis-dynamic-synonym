package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import com.bellszhu.elasticsearch.plugin.DynamicSynonymPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.PathUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

/**
 * ClassName: ReloadProperty
 * Description:
 * date: 2022/9/1 9:33
 *
 * @author sew
 */
public class ReloadProperty {
    private static Logger logger = LogManager.getLogger("ReloadProperty");

    private final static String DB_PROPERTIES = "jdbc-reload.properties";
    private Path conf_dir;
    private Properties props;
    private static ReloadProperty singleton;
    private ReloadProperty() {
        this.props = new Properties();
        // 加载 jdbc.properties 文件
        loadJdbcProperties();
    }
    /**
     * 加载 jdbc.properties
     */
    public void loadJdbcProperties() {
        //读取当前 jar 包存放的路径
        Path filePath = PathUtils.get(new File(DynamicSynonymPlugin.class.getProtectionDomain().getCodeSource()
                .getLocation().getPath())
                .getParent(), "config")
                .toAbsolutePath();
        this.conf_dir = filePath.resolve(DB_PROPERTIES);

        //判断文件是否存在
        File configFile = conf_dir.toFile();
        InputStream input = null;
        try {
            input = new FileInputStream(configFile);
        } catch (FileNotFoundException e) {
            logger.info("jdbc-reload.properties 数据库配置文件没有找到， " + e);
        }
        if (input != null) {
            try {
                props.load(input);
            } catch (IOException e) {
                logger.error("数据库配置文件 jdbc-reload.properties 加载失败，" + e);
            }
        }
    }

    public String getProperty(String key){
        if(props!=null){
            return props.getProperty(key);
        }
        return null;
    }

    public static ReloadProperty getSingleton() {
        if (singleton == null) {
            synchronized (ReloadProperty.class) {
                if (singleton == null) {
                    logger.info("---------- Dictionary initial start ---------");

                    singleton = new ReloadProperty();
        }}}
        return singleton;
    }
}