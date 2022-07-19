package com.trino.on.yarn.executor;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TrinoJdbc {

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
            statement.execute(sql);
        } finally {
            IoUtil.close(statement);
            IoUtil.close(connection);
        }

    }
}
