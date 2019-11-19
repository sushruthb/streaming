package com.struct.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcExample {
    /**
     * HiveServer2 JDBC driver name
     */
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";


    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection conn = DriverManager.getConnection("jdbc:hive2://10.76.111.129:10000/default", "hive", "");
        Statement stmt = conn.createStatement();
        // show tables
        String sql = "SHOW databases";
        System.out.println("Running: " + sql);
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();

        String sql2 = "select * from testdb.test_table";
        System.out.println("Running: " + sql2);
        ResultSet rs2 = stmt.executeQuery(sql2);

        ResultSetMetaData rsmd = rs2.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (rs2.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1)
                    System.out.print(",  ");
                String columnValue = rs2.getString(i);
                System.out.print(rsmd.getColumnName(i) + " " + columnValue);
            }
            System.out.println("");
        }

        rs2.close();
        conn.close();
    }


}