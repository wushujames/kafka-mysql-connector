package org.wushujames.connect.mysql;

import java.math.BigInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.BigIntColumnDef;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.ColumnType;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;

/**
 * 
 * DataConverter handles translating Maxwell schemas to Kafka Connect schemas and row data to Kafka
 * Connect records.
 * 
 * @author jylcheng
 *
 */
public class DataConverter {

    public static Schema convertPrimaryKeySchema(Table table) {
        String tableName = table.getName();
        String databaseName = table.getDatabase().getName();
        SchemaBuilder pkBuilder = SchemaBuilder.struct().name(databaseName + "." + tableName + ".pk");

        for (String pk : table.getPKList()) {
            int columnNumber = table.findColumnIndex(pk);
            addFieldSchema(table, columnNumber, pkBuilder);
        }
        return pkBuilder.build();
    }

    public static Schema convertRowSchema(Table table) {
        String tableName = table.getName();
        String databaseName = table.getDatabase().getName();
        SchemaBuilder builder = SchemaBuilder.struct().name(databaseName + "." + tableName);

        for (int columnNumber = 0; columnNumber < table.getColumnList().size(); columnNumber++) {
            addFieldSchema(table, columnNumber, builder);
        }
        return builder.build();
    }

    private static void addFieldSchema(Table table, int columnNumber,
            SchemaBuilder builder) {
        // TODO Auto-generated method stub
        ColumnDef def = table.getColumnList().get(columnNumber);
        String columnName = def.getName();
        ColumnType type = def.getType();
        switch (type) {
        case TINYINT:
            builder.field(columnName, Schema.INT16_SCHEMA);
            break;
        case INT:
            builder.field(columnName, Schema.INT32_SCHEMA);
            break;
        case CHAR:
            builder.field(columnName, Schema.STRING_SCHEMA);
            break;
        case BIGINT:
            builder.field(columnName, Schema.INT64_SCHEMA);
            break;
        default:
            throw new RuntimeException("unsupported type");
        }

    }

    static Struct convertPrimaryKeyData(Schema pkSchema, Table table, Row row) {
        Struct pkStruct = new Struct(pkSchema);

        for (String pk : table.getPKList()) {
            int idx = table.findColumnIndex(pk);

            Column column = row.getColumns().get(idx);
            ColumnDef def = table.getColumnList().get(idx);

            addFieldData(pkStruct, def, column);
        }
        return pkStruct;
    }

    private static void addFieldData(Struct struct, ColumnDef columnDef,
            Column column) {
        ColumnType type = columnDef.getType();
        String columnName = columnDef.getName();
        Object columnValue = column.getValue();
        switch (type) {
        case TINYINT:
            IntColumnDef shortIntDef = (IntColumnDef) columnDef;
            Long l1 = shortIntDef.toLong(columnValue);
            struct.put(columnName, l1.shortValue());
            break;
        case INT:
            IntColumnDef intDef = (IntColumnDef) columnDef;
            Long l2 = intDef.toLong(columnValue);
            struct.put(columnName, l2.intValue());
            break;
        case CHAR:
            StringColumnDef strDef = (StringColumnDef) columnDef;
            String s = strDef.toString(columnValue);
            struct.put(columnName, s);
            break;
        case BIGINT:
            BigIntColumnDef bigIntDef = (BigIntColumnDef) columnDef;
            BigInteger bigInt = bigIntDef.toNumeric(columnValue);
            struct.put(columnName, bigInt.longValue());
            break;

        default:
            throw new RuntimeException("unsupported type");
        }
    }

    static Struct convertRowData(Schema rowSchema, Table table, Row row) {
        Struct rowStruct = new Struct(rowSchema);

        for (int columnNumber = 0; columnNumber < table.getColumnList().size(); columnNumber++) {
            Column column = row.getColumns().get(columnNumber);
            ColumnDef def = table.getColumnList().get(columnNumber);

            addFieldData(rowStruct, def, column);
        }
        return rowStruct;
    }

}
