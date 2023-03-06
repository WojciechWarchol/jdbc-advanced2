package com.wojto.jdbcadvanced.thread;

import com.wojto.jdbcadvanced.data.DBUtils;
import com.wojto.jdbcadvanced.data.Table;
import org.springframework.beans.factory.annotation.Value;

import java.sql.*;
import java.util.concurrent.Callable;

public class InsertionThread implements Callable {

    private String USERNAME;
    private String PASSWORD;
    private String CONN_STRING;
    private Table table;
    private int rowsPerInsert;
    private int rowsToInsert;

    public InsertionThread(String USERNAME, String PASSWORD, String CONN_STRING, Table table, int rowsPerInsert, int rowsToInsert) {
        this.USERNAME = USERNAME;
        this.PASSWORD = PASSWORD;
        this.CONN_STRING = CONN_STRING;
        this.table = table;
        this.rowsPerInsert = rowsPerInsert;
        this.rowsToInsert = rowsToInsert;
    }

    @Override
    public String call() {
        System.out.println("Initializing Insert Thread for table: " + table.getTableName());
        long start = System.currentTimeMillis();

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            connection = DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            for (int i = 0; i < rowsToInsert; i += rowsPerInsert) {
                String insertCommand = DBUtils.createRandomRowInsertsForTable(table, rowsPerInsert);
                    statement.executeQuery(insertCommand);
            }

            if (resultSet != null) resultSet.close();
            if (statement != null) statement.close();
            if (connection != null) connection.close();

        } catch (SQLException e) {
            System.out.println("Exception");
            System.out.println(e.getMessage());
            return "Data insertion for table: " + table.getTableName() + " failed.";
        }

        long timeElapsedSinceStartOfThread = System.currentTimeMillis() - start;
        System.out.println("Thread for table: " + table.getTableName() + " finished. Time elapsed: " + timeElapsedSinceStartOfThread);
        return "Data insertion for table: " + table.getTableName() + " succeeded!";
    }
}
