package com.trino.on.yarn.executor;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TrinoJdbc {
    protected static final Log LOG = LogFactory.getLog(TrinoJdbc.class);
    public static final String JDBC_TRINO = "jdbc:trino://{}:{}/hive";

    public static void run(String ip, Integer port, String email, String sql) throws ClassNotFoundException, SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Class.forName("io.trino.jdbc.TrinoDriver");
            if (StrUtil.isEmpty(email)) {
                email = "root";
            }
            connection = DriverManager.getConnection(StrUtil.format(JDBC_TRINO, ip, port), email, null);
            statement = connection.createStatement();
            for (String tmp : StrUtil.split(sql, ";")) {
                LOG.warn("execute sql:" + tmp);
                execute(statement, tmp);
            }
        } finally {
            IoUtil.close(statement);
            IoUtil.close(connection);
        }
    }

    public static void execute(Statement statement, String sql) throws SQLException {
        try {
            statement.execute(sql);
        } catch (SQLException e) {
            if (e.getMessage().contains("initializing")) {
                ThreadUtil.sleep(500);
                execute(statement, sql);
            } else {
                throw e;
            }
        }
    }
}
