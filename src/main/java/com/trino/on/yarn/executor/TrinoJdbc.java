package com.trino.on.yarn.executor;

import cn.hutool.core.util.StrUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TrinoJdbc {

    public static final String JDBC_TRINO = "jdbc:trino://{}:{}/hive";

    public static void run(String ip, Integer port, String email, String sql) throws ClassNotFoundException, SQLException {
        Class.forName("io.trino.jdbc.TrinoDriver");
        Properties properties = new Properties();
        properties.setProperty("user", email);
        Connection connection = DriverManager.getConnection(StrUtil.format(JDBC_TRINO, ip, port), properties);
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }
}
