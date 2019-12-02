package com.lovecws.mumu.flink.common.mapper;

import com.lovecws.mumu.flink.common.config.MybatisConfig;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.enums.SqlMethod;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.toolkit.*;
import com.baomidou.mybatisplus.core.toolkit.sql.SqlHelper;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionUtils;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @program: act-industry-data-common
 * @description: BaseServiceImpl
 * @author: 甘亮
 * @create: 2019-11-08 09:53
 **/
public class BaseServiceImpl<M extends BaseMapper<T>, T> implements IService<T>, Serializable {

    public M baseMapper;

    public BaseServiceImpl() {
        Class<M> tClass = currentMapperClass();
        baseMapper = MybatisConfig.getMapper(tClass);
    }

    public void setBaseMapper(M baseMapper) {
        this.baseMapper = baseMapper;
    }

    /**
     * <p>
     * 判断数据库操作是否成功
     * </p>
     *
     * @param result 数据库操作返回影响条数
     * @return boolean
     */
    protected boolean retBool(Integer result) {
        return SqlHelper.retBool(result);
    }

    protected Class<T> currentModelClass() {
        return ReflectionKit.getSuperClassGenericType(getClass(), 1);
    }

    protected Class<M> currentMapperClass() {
        return (Class<M>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * <p>
     * 批量操作 SqlSession
     * </p>
     */
    protected SqlSession sqlSessionBatch() {
        return MybatisConfig.getSqlSession();
    }

    /**
     * 释放sqlSession
     *
     * @param sqlSession session
     */
    protected void closeSqlSession(SqlSession sqlSession) {
        SqlSessionUtils.closeSqlSession(sqlSession, GlobalConfigUtils.currentSessionFactory(currentModelClass()));
    }

    /**
     * 获取SqlStatement
     *
     * @param sqlMethod
     * @return
     */
    protected String sqlStatement(SqlMethod sqlMethod) {
        return SqlHelper.table(currentModelClass()).getSqlStatement(sqlMethod.getMethod());
    }

    @Override
    public boolean save(T entity) {
        try (SqlSession batchSqlSession = sqlSessionBatch()) {
            M mapper = batchSqlSession.getMapper(currentMapperClass());
            boolean retBool = retBool(mapper.insert(entity));
            batchSqlSession.commit();
            batchSqlSession.close();
            return retBool;
        }
    }

    /**
     * 批量插入
     *
     * @param entityList
     * @param batchSize
     * @return
     */
    @Override
    public boolean saveBatch(Collection<T> entityList, int batchSize) {
        int i = 0;
        String sqlStatement = sqlStatement(SqlMethod.INSERT_ONE);
        try (SqlSession batchSqlSession = sqlSessionBatch()) {
            for (T anEntityList : entityList) {
                batchSqlSession.insert(sqlStatement, anEntityList);
                if (i >= 1 && i % batchSize == 0) {
                    batchSqlSession.flushStatements();
                }
                i++;
            }
            batchSqlSession.commit();
            batchSqlSession.flushStatements();
            batchSqlSession.close();
        }
        return true;
    }

    /**
     * <p>
     * TableId 注解存在更新记录，否插入一条记录
     * </p>
     *
     * @param entity 实体对象
     * @return boolean
     */
    @Override
    public boolean saveOrUpdate(T entity) {
        if (null != entity) {
            Class<?> cls = entity.getClass();
            TableInfo tableInfo = TableInfoHelper.getTableInfo(cls);
            if (null != tableInfo && StringUtils.isNotEmpty(tableInfo.getKeyProperty())) {
                Object idVal = ReflectionKit.getMethodValue(cls, entity, tableInfo.getKeyProperty());
                if (StringUtils.checkValNull(idVal)) {
                    return save(entity);
                } else {
                    /*
                     * 更新成功直接返回，失败执行插入逻辑
                     */
                    return updateById(entity) || save(entity);
                }
            } else {
                throw ExceptionUtils.mpe("Error:  Can not execute. Could not find @TableId.");
            }
        }
        return false;
    }

    @Override
    public boolean saveOrUpdateBatch(Collection<T> entityList) {
        return saveOrUpdateBatch(entityList, 30);
    }

    @Override
    public boolean saveOrUpdateBatch(Collection<T> entityList, int batchSize) {
        if (CollectionUtils.isEmpty(entityList)) {
            throw new IllegalArgumentException("Error: entityList must not be empty");
        }
        Class<?> cls = null;
        TableInfo tableInfo = null;
        int i = 0;
        try (SqlSession batchSqlSession = sqlSessionBatch()) {
            for (T anEntityList : entityList) {
                if (i == 0) {
                    cls = anEntityList.getClass();
                    tableInfo = TableInfoHelper.getTableInfo(cls);
                }
                if (null != tableInfo && StringUtils.isNotEmpty(tableInfo.getKeyProperty())) {
                    Object idVal = ReflectionKit.getMethodValue(cls, anEntityList, tableInfo.getKeyProperty());
                    if (StringUtils.checkValNull(idVal)) {
                        String sqlStatement = sqlStatement(SqlMethod.INSERT_ONE);
                        batchSqlSession.insert(sqlStatement, anEntityList);
                    } else {
                        String sqlStatement = sqlStatement(SqlMethod.UPDATE_BY_ID);
                        MapperMethod.ParamMap<T> param = new MapperMethod.ParamMap<>();
                        param.put(Constants.ENTITY, anEntityList);
                        batchSqlSession.update(sqlStatement, param);
                        //不知道以后会不会有人说更新失败了还要执行插入 😂😂😂
                    }
                    if (i >= 1 && i % batchSize == 0) {
                        batchSqlSession.flushStatements();
                    }
                    i++;
                } else {
                    throw ExceptionUtils.mpe("Error:  Can not execute. Could not find @TableId.");
                }
                batchSqlSession.flushStatements();
                batchSqlSession.commit();
            }
        }
        return true;
    }

    @Override
    public boolean removeById(Serializable id) {
        try (SqlSession batchSqlSession = sqlSessionBatch()) {
            M mapper = batchSqlSession.getMapper(currentMapperClass());
            boolean retBool = SqlHelper.delBool(mapper.deleteById(id));
            batchSqlSession.commit();
            batchSqlSession.close();
            return retBool;
        }
    }

    @Override
    public boolean removeByMap(Map<String, Object> columnMap) {
        if (ObjectUtils.isEmpty(columnMap)) {
            throw ExceptionUtils.mpe("removeByMap columnMap is empty.");
        }
        try (SqlSession batchSqlSession = sqlSessionBatch()) {
            M mapper = batchSqlSession.getMapper(currentMapperClass());
            boolean retBool = SqlHelper.delBool(mapper.deleteByMap(columnMap));
            batchSqlSession.commit();
            batchSqlSession.close();
            return retBool;
        }
    }

    @Override
    public boolean remove(Wrapper<T> wrapper) {
        try (SqlSession batchSqlSession = sqlSessionBatch()) {
            M mapper = batchSqlSession.getMapper(currentMapperClass());
            boolean retBool = SqlHelper.delBool(mapper.delete(wrapper));
            batchSqlSession.commit();
            batchSqlSession.close();
            return retBool;
        }
    }

    @Override
    public boolean removeByIds(Collection<? extends Serializable> idList) {
        try (SqlSession batchSqlSession = sqlSessionBatch()) {
            M mapper = batchSqlSession.getMapper(currentMapperClass());
            boolean retBool = SqlHelper.delBool(mapper.deleteBatchIds(idList));
            batchSqlSession.commit();
            batchSqlSession.close();
            return retBool;
        }
    }

    @Override
    public boolean updateById(T entity) {
        try (SqlSession batchSqlSession = sqlSessionBatch()) {
            M mapper = batchSqlSession.getMapper(currentMapperClass());
            boolean retBool = retBool(mapper.updateById(entity));
            batchSqlSession.commit();
            batchSqlSession.close();
            return retBool;
        }
    }

    @Override
    public boolean update(T entity, Wrapper<T> updateWrapper) {
        try (SqlSession batchSqlSession = sqlSessionBatch()) {
            M mapper = batchSqlSession.getMapper(currentMapperClass());
            boolean retBool = retBool(mapper.update(entity, updateWrapper));
            batchSqlSession.commit();
            batchSqlSession.close();
            return retBool;
        }
    }

    @Override
    public boolean updateBatchById(Collection<T> entityList, int batchSize) {
        if (CollectionUtils.isEmpty(entityList)) {
            throw new IllegalArgumentException("Error: entityList must not be empty");
        }
        int i = 0;
        String sqlStatement = sqlStatement(SqlMethod.UPDATE_BY_ID);
        try (SqlSession batchSqlSession = sqlSessionBatch()) {
            for (T anEntityList : entityList) {
                MapperMethod.ParamMap<T> param = new MapperMethod.ParamMap<>();
                param.put(Constants.ENTITY, anEntityList);
                batchSqlSession.update(sqlStatement, param);
                if (i >= 1 && i % batchSize == 0) {
                    batchSqlSession.flushStatements();
                }
                i++;
            }
            batchSqlSession.commit();
            batchSqlSession.flushStatements();
        }
        return true;
    }

    @Override
    public T getById(Serializable id) {
        return baseMapper.selectById(id);
    }

    @Override
    public Collection<T> listByIds(Collection<? extends Serializable> idList) {
        return baseMapper.selectBatchIds(idList);
    }

    @Override
    public Collection<T> listByMap(Map<String, Object> columnMap) {
        return baseMapper.selectByMap(columnMap);
    }

    @Override
    public T getOne(Wrapper<T> queryWrapper, boolean throwEx) {
        if (throwEx) {
            return baseMapper.selectOne(queryWrapper);
        }
        return SqlHelper.getObject(baseMapper.selectList(queryWrapper));
    }

    @Override
    public Map<String, Object> getMap(Wrapper<T> queryWrapper) {
        return SqlHelper.getObject(baseMapper.selectMaps(queryWrapper));
    }

    @Override
    public Object getObj(Wrapper<T> queryWrapper) {
        return SqlHelper.getObject(baseMapper.selectObjs(queryWrapper));
    }

    @Override
    public int count(Wrapper<T> queryWrapper) {
        return SqlHelper.retCount(baseMapper.selectCount(queryWrapper));
    }

    @Override
    public List<T> list(Wrapper<T> queryWrapper) {
        return baseMapper.selectList(queryWrapper);
    }

    @Override
    public IPage<T> page(IPage<T> page, Wrapper<T> queryWrapper) {
        return baseMapper.selectPage(page, queryWrapper);
    }

    @Override
    public List<Map<String, Object>> listMaps(Wrapper<T> queryWrapper) {
        return baseMapper.selectMaps(queryWrapper);
    }

    @Override
    public List<Object> listObjs(Wrapper<T> queryWrapper) {
        return baseMapper.selectObjs(queryWrapper).stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public IPage<Map<String, Object>> pageMaps(IPage<T> page, Wrapper<T> queryWrapper) {
        return baseMapper.selectMapsPage(page, queryWrapper);
    }
}
