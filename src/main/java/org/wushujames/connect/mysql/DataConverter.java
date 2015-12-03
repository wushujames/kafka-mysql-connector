package org.wushujames.connect.mysql;

import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaBuilder;
import org.apache.kafka.copycat.data.Struct;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.zendesk.maxwell.schema.Table;
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
        case BOOL:
        case BOOLEAN:
            builder.field(columnName, Schema.BOOLEAN_SCHEMA);
            break;
        case BIT:
            builder.field(columnName, Schema.INT8_SCHEMA);
            break;
        case SMALLINT:
            builder.field(columnName, Schema.INT16_SCHEMA);
            break;
        case MEDIUMINT:
        case INT:
            builder.field(columnName, Schema.INT32_SCHEMA);
            break;
        case CHAR:
            builder.field(columnName, Schema.STRING_SCHEMA);
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
		switch (columnDef.getType()) {
		case INT:
		    IntColumnDef intDef = (IntColumnDef) columnDef;
		    Long l = intDef.toLong(column.getValue());
		    struct.put(columnDef.getName(), l.intValue());
		    break;
		case CHAR:
		    StringColumnDef strDef = (StringColumnDef) columnDef;
		    String s = strDef.toString(column.getValue());
		    struct.put(columnDef.getName(), s);
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
