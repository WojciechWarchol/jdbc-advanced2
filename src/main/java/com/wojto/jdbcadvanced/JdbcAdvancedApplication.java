package com.wojto.jdbcadvanced;

import com.wojto.jdbcadvanced.data.Column;
import com.wojto.jdbcadvanced.data.DBUtils;
import com.wojto.jdbcadvanced.data.Table;
import com.wojto.jdbcadvanced.thread.InsertionThread;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
public class JdbcAdvancedApplication implements CommandLineRunner {

    @Value("${connection.username}")
    private String USERNAME;
    @Value("${connection.password}")
    private String PASSWORD;
    @Value("${connection.link}")
    private String CONN_STRING;

    @Value("${parameters.table-number}")
    private int TABLE_NUM;
    @Value("${parameters.column-number}")
    private int COLUMN_NUM;
    @Value("${parameters.rows-to-insert}")
    private int ROWS_TO_INSERT;
    @Value("${parameters.rows-per-insert}")
    private int ROWS_PER_INSERT;

    @Value("${parameters.multithreaded}")
    private boolean MULTITHREADED;


    public static void main(String[] args) throws SQLException, InterruptedException {
        SpringApplication.run(JdbcAdvancedApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        System.out.println("CONN_STRING:" + CONN_STRING);

        long start = System.currentTimeMillis();
        System.out.println("Started Application");

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        connection = DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        statement.executeQuery("DROP SCHEMA PUBLIC CASCADE;");
        long timeElapsedToTableDeletion = System.currentTimeMillis() - start;
        System.out.println("Tables deleted. Time elapsed: " + timeElapsedToTableDeletion);

        List<Table> tables = new ArrayList<>();

        for (int i = 1; i <= TABLE_NUM; i++) {
            Table table = new Table("Table_" + i);
            for (int j = 1; j <= COLUMN_NUM; j++) {
                table.addColumn(Column.getColumnWithNameAndRandomType("Column_" + j));
            }
            tables.add(table);
        }

        StringBuilder tableCreationString = new StringBuilder();
        for (Table t : tables) {
            tableCreationString.append(t.getSqlCreationCommand());
        }

        System.out.println(tableCreationString.toString());
        statement.executeQuery(tableCreationString.toString());
        long timeElapsedToTableCreation = System.currentTimeMillis() - start;
        System.out.println("Tables created. Time elapsed: " + timeElapsedToTableCreation);

        if (MULTITHREADED) {
            ExecutorService executorService = Executors.newFixedThreadPool(10);
            List<Callable<String>> insertionTasks = new ArrayList<>();
            for (Table t : tables) {
                Callable<String> insertionTask = new InsertionThread(USERNAME, PASSWORD, CONN_STRING, t, ROWS_PER_INSERT, ROWS_TO_INSERT);
                insertionTasks.add(insertionTask);
            }

            executorService.invokeAll(insertionTasks);
            executorService.shutdown();
        } else {
            for (Table t : tables) {
                long dataInsertionStart = System.currentTimeMillis();
                for (int i = 0; i < ROWS_TO_INSERT; i += ROWS_PER_INSERT) {
                    String insertCommand = DBUtils.createRandomRowInsertsForTable(t, ROWS_PER_INSERT);
                    statement.executeQuery(insertCommand);
                }
                long timeElapsedToDataInsert = System.currentTimeMillis() - dataInsertionStart;
                System.out.println("Data inserted to table " + t.getTableName() + "Time elapsed: " + timeElapsedToDataInsert);
            }
        }

        System.out.println("Checking if tables created by querying last table.");
        resultSet = statement.executeQuery("SELECT * FROM Table_" + TABLE_NUM + " LIMIT 10");
        DBUtils.printResultSet(resultSet);

        if (resultSet != null) {
            resultSet.close();
        }
        statement.close();
        connection.close();

        long timeAtFinish = System.currentTimeMillis() - start;
        System.out.println("Closing application. Total elapsed time: " + timeAtFinish);

    }
}
