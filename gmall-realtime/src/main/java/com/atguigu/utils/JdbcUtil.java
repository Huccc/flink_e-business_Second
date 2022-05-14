package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * select count(*) from t1;   一行一列
 * select id from t1;       多行一列
 * select id,name from t1 where id =1001; id 是唯一键           一行多列
 * select * from t1;              多行多列
 */
public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception {

        // 创建集合用于存放查询结果
        ArrayList<T> result = new ArrayList<>();

        // 编译SQL
        // 工具类中的异常  更多采用抛的方式
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        // 执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        // 获取列名信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 遍历resultSet，将每行查询的数据封装为   T   对象
        // next 即查看下一个对象，同时指针向下一步
        while (resultSet.next()) {
            // 构建T对象
            T t = clz.newInstance();

            for (int i = 1; i < columnCount + 1; i++) {
                String columnName = metaData.getColumnName(i);
                Object object = resultSet.getObject(columnName);

                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                // 给T对象进行属性赋值
                BeanUtils.setProperty(t, columnName, object);
            }

            // 将T对象添加至集合
            result.add(t);

        }

        // 关闭资源对象
        resultSet.close();
        preparedStatement.close();

        // 返回结果
        return result;
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        List<JSONObject> queryList = queryList(connection, "select * from GMALL210826_REALTIME.DIM_USER_INFO", JSONObject.class, false);

        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }

        connection.close();
    }
}






























