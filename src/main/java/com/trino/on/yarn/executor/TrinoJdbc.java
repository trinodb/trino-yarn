package com.trino.on.yarn.executor;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.List;

public class TrinoJdbc {
    protected static final Log LOG = LogFactory.getLog(TrinoJdbc.class);
    public static final String JDBC_TRINO = "jdbc:trino://{}:{}/hive";

    public static void run(String ip, Integer port, String email, String sqls) throws ClassNotFoundException, SQLException {
        Connection connection = null;

        try {
            Class.forName("io.trino.jdbc.TrinoDriver");
            if (StrUtil.isEmpty(email)) {
                email = "root";
            }
            connection = DriverManager.getConnection(StrUtil.format(JDBC_TRINO, ip, port), email, null);

            for (String sql : StrUtil.split(sqls, ";")) {
                LOG.warn("execute sql:" + sql);
                execute(connection, sql);
            }
        } finally {
            IoUtil.close(connection);
        }
    }

    public static void execute(Connection connection, String sql) throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            boolean execute = statement.execute(sql);
            // TODO: 2022/7/27 这里可能会出现问题,暂不启用
            //resultSet = printResults(sql, statement, execute);
        } catch (SQLException e) {
            String message = e.getMessage();
            if (message.contains("initializing")) {
                ThreadUtil.sleep(500);
                execute(connection, sql);
            } else {
                throw e;
            }
        } finally {
            IoUtil.close(resultSet);
            IoUtil.close(statement);
        }
    }

    private static ResultSet printResults(String sql, Statement statement, boolean execute) throws SQLException {
        ResultSet resultSet = null;
        if (execute) {
            resultSet = statement.getResultSet();
            if (statement.getUpdateCount() < 0 && resultSet.getMetaData().getColumnCount() <= 0) {
                LOG.info("current result is a ResultSet Object , but there are no more results :" + sql);
            } else {
                ResultSet finalResultSet = resultSet;
                ThreadUtil.execute(() -> {
                    try {
                        printResults(finalResultSet);
                    } catch (SQLException e) {
                        LOG.error("printResultSet error", e);
                    }
                });
            }
        }
        return resultSet;
    }

    public static void printResults(ResultSet rs) throws SQLException {
        List<String> columns = CollUtil.newArrayList();
        List<String> rows = CollUtil.newArrayList();
        try {
            ResultSetMetaData md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                columns.add(md.getColumnName(i));
            }
            System.out.println(StrUtil.join("  |  ", columns));
            while (rs.next()) {//遍历查询结果，获取每一列字符串长度的最大值，保存在数组中
                rows.clear();
                for (int i = 1; i <= columns.size(); i++) {
                    rows.add(String.valueOf(rs.getObject(i)));
                }
                System.out.println(StrUtil.join("  |  ", rows));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}