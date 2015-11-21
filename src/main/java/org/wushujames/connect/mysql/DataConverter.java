package org.wushujames.connect.mysql;

import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaBuilder;

import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

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
            addFieldSchema(table, columnNumber, pk, pkBuilder);
        }
        return pkBuilder.build();
    }

    public static Schema convertRowSchema(Table table) {
        String tableName = table.getName();
        String databaseName = table.getDatabase().getName();
        SchemaBuilder builder = SchemaBuilder.struct().name(databaseName + "." + tableName);

        for (int columnNumber = 0; columnNumber < table.getColumnList().size(); columnNumber++) {
            String columnName = table.getColumnList().get(columnNumber).getName();
            addFieldSchema(table, columnNumber, columnName , builder);
        }
        return builder.build();
    }

    private static void addFieldSchema(Table table, int columnNumber,
            String columnName, SchemaBuilder builder) {
        // TODO Auto-generated method stub
        ColumnDef def = table.getColumnList().get(columnNumber);
        String type = def.getType();
        switch (type) {
        case "bool":
        case "boolean":
            builder.field(columnName, Schema.BOOLEAN_SCHEMA);
            break;
        case "bit":
        case "tinyint":
            builder.field(columnName, Schema.INT8_SCHEMA);
            break;
        case "smallint":
            builder.field(columnName, Schema.INT16_SCHEMA);
            break;
        case "mediumint":
        case "int":
            builder.field(columnName, Schema.INT32_SCHEMA);
            break;
        case "char":
            builder.field(columnName, Schema.STRING_SCHEMA);
            break;
        default:
            throw new RuntimeException("unsupported type");
        }

    }
}
