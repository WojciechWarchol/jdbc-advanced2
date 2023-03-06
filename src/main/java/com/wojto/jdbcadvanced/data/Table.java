package com.wojto.jdbcadvanced.data;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter @Setter @NoArgsConstructor
public class Table {

    private String tableName;
    private List<Column> columns;

    public Table(String tableName) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();
    }

    public void addColumn(Column column) {
        columns.add(column);
    }

    public String getSqlCreationCommand() {
        StringBuilder sb = new StringBuilder("CREATE TABLE " + tableName + " ( ");
        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            sb.append(c.getSqlCommand());
            if ( i < columns.size() - 1 ) sb.append(", ");
        }
        sb.append(");\n");
        return sb.toString();
    }
}
