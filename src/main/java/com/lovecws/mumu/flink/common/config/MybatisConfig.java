package com.lovecws.mumu.flink.common.config;

import com.lovecws.mumu.flink.common.jdbc.DataSourceUtil;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.plugins.PaginationInterceptor;
import com.baomidou.mybatisplus.extension.plugins.PerformanceInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.session.SqlSessionManager;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import javax.sql.DataSource;
import java.io.InputStream;

/**
 * @program: flinkdemo
 * @description: mybatis配置
 * @author: 甘亮
 * @create: 2019-11-07 17:02
 **/
@Slf4j
public class MybatisConfig {

    private static SqlSessionFactory sqlSessionFactory = null;
    private static SqlSessionManager sqlSessionManager = null;
    private static final Object object = new Object();

    private static final String[] MAPPERS = new String[]{"mapper/CloudPlatformMapper.xml",
            "mapper/CorpTypeMapper.xml", "mapper/SyslogCorpMapper.xml", "mapper/SyslogDictMapper.xml"};
    private static final String MAPPERPACKAGE = "com.act.able.industry.data.common.mapper";

    public static void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
        MybatisConfig.sqlSessionFactory = sqlSessionFactory;
    }

    public static SqlSessionFactory getsqlSessionFactory() {
        if (sqlSessionFactory == null) {
            synchronized (object) {
                if (sqlSessionFactory == null) {
                    try {
                        MybatisConfiguration configuration = new MybatisConfiguration();
                        configuration.addMappers(MAPPERPACKAGE);
                        for (String resource : MAPPERS) {
                            InputStream inputStream = Resources.getResourceAsStream(resource);
                            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource, configuration.getSqlFragments());
                            mapperParser.parse();
                        }

                        DataSource dataSource = DataSourceUtil.getJdbcConfig().getDataSource();
                        Environment environment = new Environment("1", new JdbcTransactionFactory(), dataSource);
                        configuration.setEnvironment(environment);

                        configuration.addInterceptor(new PaginationInterceptor());

                        PerformanceInterceptor performanceInterceptor = new PerformanceInterceptor();
                        //<!-- SQL 执行性能分析，开发环境使用，线上不推荐。 maxTime 指的是 sql 最大执行时长 -->
                        performanceInterceptor.setMaxTime(1200000);
                        //<!--SQL是否格式化 默认false-->
                        performanceInterceptor.setFormat(false);
                        //configuration.addInterceptor(performanceInterceptor);

                        configuration.setCacheEnabled(true);
                        sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
                    } catch (Exception ex) {
                        log.error(ex.getLocalizedMessage());
                        return getsqlSessionFactory();
                    }
                }
            }
        }
        sqlSessionManager = SqlSessionManager.newInstance(sqlSessionFactory);
        sqlSessionManager.startManagedSession();

        return sqlSessionFactory;
    }

    public static SqlSessionManager getSqlSessionManager() {
        if (sqlSessionManager == null) {
            getsqlSessionFactory();
        }
        return sqlSessionManager;
    }


    public static SqlSession getSqlSession() {
        SqlSessionManager sqlSessionManager = getSqlSessionManager();
        return sqlSessionManager.openSession();
    }

    public static <T> T getSqlMapper(Class<T> clazz) {
        try {
            return getSqlSessionManager().getMapper(clazz);
        } catch (Exception ex) {
            MybatisConfig.sqlSessionFactory = null;
            return MybatisConfig.getSqlSession().getMapper(clazz);
        }
    }

    public static <T> T getMapper(Class<T> clazz) {
        SqlSessionManager sqlSessionManager = getSqlSessionManager();
        return sqlSessionManager.getMapper(clazz);
    }
}
