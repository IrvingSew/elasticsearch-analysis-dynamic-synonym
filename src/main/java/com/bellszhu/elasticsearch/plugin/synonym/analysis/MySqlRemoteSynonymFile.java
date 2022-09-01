package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import com.alibaba.druid.pool.DruidDataSource;
import com.bellszhu.elasticsearch.plugin.DynamicSynonymPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.env.Environment;

import java.io.*;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

/**
 * ClassName: DBRemoteSynonymFile
 * Description:
 * date: 2022/7/28 15:09
 *
 * @author sew
 */
public class MySqlRemoteSynonymFile implements SynonymFile{

    /**
     * 数据库配置文件名
     */
    private final static String DB_PROPERTIES = "jdbc-reload.properties";
    private static Logger logger = LogManager.getLogger("dynamic-synonym");

    private String format;

    private boolean expand;

    private boolean lenient;

    private Analyzer analyzer;

    private Environment env;

    // 数据库配置
    private String location;

    // 数据库地址
    private static final String jdbcUrl = "jdbc.url";
    // 数据库用户名
    private static final String jdbcUser = "jdbc.user";
    // 数据库密码
    private static final String jdbcPassword = "jdbc.password";
    private static final String jdbcDriver = "jdbc.driver";

    /**
     * 当前节点的同义词版本号
     */
    private long lastModified;

    private static Properties props;

    private Path conf_dir;

    private static DruidDataSource dataSource;
    private static final int maxActive = 10;
    private static final int initialSize = 1;
    private static final int maxWait = 6000;
    private static final int minIdle = 2;

    static {
        dataSource= new DruidDataSource();
        dataSource.setUrl(getUrl());
        dataSource.setDriverClassName(getDriver());
        dataSource.setUsername(getUsername());
        dataSource.setPassword(getPassword());
        dataSource.setMaxActive(maxActive);
        dataSource.setInitialSize(initialSize);
        dataSource.setMaxWait(maxWait);
        dataSource.setMinIdle(minIdle);
        dataSource.setConnectionErrorRetryAttempts(5);
        dataSource.setBreakAfterAcquireFailure(true);
    }

    public static String getUrl() {
        return  ReloadProperty.getSingleton().getProperty(jdbcUrl); }
    public static String getUsername() {
        return  ReloadProperty.getSingleton().getProperty(jdbcUser);
    }
    public static String getPassword() {
        return  ReloadProperty.getSingleton().getProperty(jdbcPassword);
    }
    public static String getDriver() { return ReloadProperty.getSingleton().getProperty(jdbcDriver); }

    MySqlRemoteSynonymFile(Environment env, Analyzer analyzer,
                           boolean expand, boolean lenient, String format, String location) {
        this.analyzer = analyzer;
        this.expand = expand;
        this.format = format;
        this.lenient = lenient;
        this.env = env;
        this.location = location;
        this.props = new Properties();

        logger.info("------ MySqlRemoteSynonymFile struct load start -------");

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
        isNeedReloadSynonymMap();
    }

    /**
     * 加载同义词词典至SynonymMap中
     * @return SynonymMap
     */
    @Override
    public SynonymMap reloadSynonymMap() {
        try {
            logger.info("start reload local synonym from {}.", location);
            Reader rulesReader = getReader();
            SynonymMap.Builder parser = RemoteSynonymFile.getSynonymParser(rulesReader, format, expand, lenient, analyzer);
            return parser.build();
        } catch (Exception e) {
            logger.error("reload local synonym {} error!", e, location);
            throw new IllegalArgumentException(
                    "could not reload local synonyms file to build synonyms", e);
        }
    }

    /**
     * 判断是否需要进行重新加载
     * @return true or false
     */
    @Override
    public boolean isNeedReloadSynonymMap() {
        try {
            logger.info("------ isNeedReloadSynonymMap! -------");

            Long lastModify = getLastModify();
            if (Objects.isNull(lastModified) || (lastModified < lastModify)) {
                lastModified = lastModify;
                return true;
            }
        } catch (Exception e) {
            logger.error(e);
        }

        return false;
    }

    /**
     * 获取同义词库最后一次修改的时间
     * 用于判断同义词是否需要进行重新加载
     *
     * @return getLastModify
     */
    public Long getLastModify() {
        Long last_modify_long = null;
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            String sql = props.getProperty("jdbc.lastModified.synonym.sql");
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                Timestamp last_modify_dt = rs.getTimestamp("last_modify_dt");
                logger.info("获取同义词库最后一次修改的时间，last_modify_dt:{}",last_modify_dt);
                last_modify_long = last_modify_dt.getTime();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeRsAndPs(rs, ps,conn);
        }
        return last_modify_long;
    }

    public ArrayList<String> getDBData() {
        ArrayList<String> arrayList = new ArrayList<>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            String sql = props.getProperty("jdbc.reload.synonym.sql");
            logger.info("正在执行SQL查询同义词列表，SQL:{}", sql);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String theWord = rs.getString("word");
                arrayList.add(theWord);
            }
        } catch (Exception e) {
            logger.error(e);
        } finally {
            closeRsAndPs(rs, ps,conn);
        }
        return arrayList;
    }

    public void closeRsAndPs(ResultSet rs, PreparedStatement ps,Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error("failed to close ResultSet", e);
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                logger.error("failed to close PreparedStatement", e);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("failed to close Connection", e);
            }
        }
    }

    /**
     * 同义词库的加载
     * @return Reader
     */
    @Override
    public Reader getReader() {

        StringBuffer sb = new StringBuffer();
        try {
            ArrayList<String> dbData = getDBData();
            for (int i = 0; i < dbData.size(); i++) {
//                logger.info("正在加载同义词:{}", dbData.get(i));
                // 获取一行一行的记录，每一条记录都包含多个词，形成一个词组，词与词之间使用英文逗号分割
                sb.append(dbData.get(i))
                        .append(System.getProperty("line.separator"));
            }
            logger.info("synony Loading :total{}", dbData.size());
        } catch (Exception e) {
            logger.error("同义词加载失败");
        }
        return new StringReader(sb.toString());
    }

}
