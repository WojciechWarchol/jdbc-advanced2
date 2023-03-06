package com.wojto.jdbcadvanced.data;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.RandomStringUtils;

import java.lang.reflect.Type;
import java.util.Random;

@Getter @Setter @NoArgsConstructor
public class Column {

    private String columnName;
    private ColumnType columnType;

    public Column(String columnName, ColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public static Column getColumnWithNameAndRandomType(String columnName) {
        return new Column(columnName, ColumnType.getRandomColumnType());
    }

    public String getSqlCommand() {
        return columnName + " " + columnType.sqlCreationCommand;
    }

    public String getRandomValueForThisColumnType() {
        switch (this.columnType) {
            case INT:
                return String.valueOf(ColumnType.random.nextInt(10000));
            case DOUBLE:
                return String.format("%.2f", ColumnType.random.nextDouble() * ColumnType.random.nextInt(1000));
            case VARCHAR:
                return "'" + RandomStringUtils.random(ColumnType.random.nextInt(17) + 3, true, false) + "'";
        }
        return null;
    }


    enum ColumnType {
        INT(Integer.class, "INT"),
        DOUBLE(Double.class, "DOUBLE"),
        VARCHAR(String.class, "VARCHAR(50)");

        public final Type columntType;
        public final String sqlCreationCommand;

        private static final Random random = new Random();
        private static final ColumnType[] typeArray = values();

        private static ColumnType getRandomColumnType() {
            return typeArray[random.nextInt(typeArray.length)];
        }

        private ColumnType(Type columntType, String sqlCreationCommand) {
            this.columntType = columntType;
            this.sqlCreationCommand = sqlCreationCommand;
        }
    }
}
