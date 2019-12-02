package com.lovecws.mumu.flink.common.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2019/3/14.
 */
public class JdbcUtil {

    private JdbcUtil() {
    }

    public static Connection getConn(String driverClass, String url, String username, String password) throws Exception {
        Class.forName(driverClass);
        return DriverManager.getConnection(url, username, password);
    }

    public static void close(Connection conn, Statement statement, ResultSet rs) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                rs = null;
            }
            if (null != statement) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    statement = null;
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    conn = null;
                }
            }

        }
    }


    public static List<Object> getObjects(String sql, @SuppressWarnings("rawtypes") Class clazz, String driverCLass, String url, String username, String password)
            throws SQLException, InstantiationException,
            IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = getConn(driverCLass, url, username, password);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            String[] colNames = getColNames(rs);

            List<Object> objects = new ArrayList<Object>();
            //获取所有的公共方法
            Method[] ms = clazz.getMethods();
            while (rs.next()) {
                //创建一个新对象
                Object object = clazz.newInstance();
                for (int i = 0; i < colNames.length; i++) {
                    String colName = colNames[i];
                    //获取set方法的方法名
                    String methodName = "set" + colName;
                    for (Method m : ms) {
                        if (methodName.equals(m.getName())) {
                            m.invoke(object, rs.getObject(colName));
                            break;
                        }
                    }
                    objects.add(object);
                }
            }
            return objects;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(conn, ps, rs);
        }
        return null;
    }

    /**
     * 根据结果集获取查询结果的别名
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    private static String[] getColNames(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        //获取查询的列数
        int count = rsmd.getColumnCount();
        String[] colNames = new String[count];
        for (int i = 1; i <= count; i++) {
            //获取查询类的别名
            colNames[i - 1] = rsmd.getColumnLabel(i);
        }
        return colNames;
    }


}
