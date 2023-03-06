package com.wojto.jdbcadvanced.data;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DBUtils {

    public static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        System.out.println("---------- Printing out ResultSet -----------");
        int columnsNumber = rsmd.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = rs.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
    }

    public static String createRandomRowInsertsForTable(Table table, int num) {
        List<Column> columns = new ArrayList<>(table.getColumns());
        StringBuilder sb = new StringBuilder("INSERT INTO " + table.getTableName() + " (");
        sb.append(columns.stream().map(Column::getColumnName).collect(Collectors.joining(", ")));
        sb.append(") \nVALUES\n");
        for (int i = 0; i < num; i++) {
            sb.append("\t(");
            sb.append(columns.stream().map(Column::getRandomValueForThisColumnType).collect(Collectors.joining(", ")));
            sb.append(")");
            String commaOrSemicolon = i < num - 1 ? ",\n" : ";";
            sb.append(commaOrSemicolon);
        }

        return sb.toString();
    }
}
