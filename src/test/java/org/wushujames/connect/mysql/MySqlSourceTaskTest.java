package org.wushujames.connect.mysql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

public class MySqlSourceTaskTest {
    private Map<String, String> config;
    private MySqlSourceTask task;
    private OffsetStorageReader offsetStorageReader;
    private SourceTaskContext context;

    private boolean verifyMocks = false;
    private Connection connection;

    @Before
    public void setup() throws IOException, SQLException {
        String mysqlHost = "10.100.172.86";
        connection = DriverManager.getConnection("jdbc:mysql://" + mysqlHost + ":3306/mysql", "root", "passwd");
        
        config = new HashMap<>();
        config.put(MySqlSourceConnector.USER_CONFIG, "maxwell");
        config.put(MySqlSourceConnector.PASSWORD_CONFIG, "XXXXXX");
        config.put(MySqlSourceConnector.PORT_CONFIG, "3306");
        config.put(MySqlSourceConnector.HOST_CONFIG, mysqlHost);
        
        task = new MySqlSourceTask();
        offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);
        context = PowerMock.createMock(SourceTaskContext.class);
        task.initialize(context);

        runSql("drop table if exists test.users");
        runSql("drop database if exists test");
    }
    
    @After
    public void teardown() {

        if (verifyMocks)
            PowerMock.verifyAll();
    }

    private void replay() {
        PowerMock.replayAll();
        verifyMocks = true;
    }

    @Test
    public void testNormalLifecycle() throws InterruptedException, IOException, SQLException {
        expectOffsetLookupReturnNone();
        replay();
        
        task.start(config);

        runSql("create database test");
        runSql("create table test.users (userId int auto_increment primary key, name char(128))");
        
        runSql("insert into test.users (name) values (\"James\");");
        List<SourceRecord> records = pollUntilRows();
        assertEquals(1, records.size());
        SourceRecord james = records.get(0);
        
        // check key schema
        Schema keySchema = james.keySchema();
        assertEquals(1, keySchema.fields().size());
        assertNotNull(keySchema.field("userid"));
        assertEquals(Schema.INT32_SCHEMA, keySchema.field("userid").schema());
        
        // check key
        Object keyObject = james.key();
        assertTrue(keyObject instanceof Struct);
        Struct key = (Struct) keyObject;
        assertEquals(1, key.get("userid"));
        
        // check value schema
        Schema valueSchema = james.valueSchema();
        assertEquals(2, valueSchema.fields().size());
        assertNotNull(valueSchema.field("userid"));
        assertEquals(Schema.INT32_SCHEMA, valueSchema.field("userid").schema());
        assertNotNull(valueSchema.field("name"));
        assertEquals(Schema.STRING_SCHEMA, valueSchema.field("name").schema());
        
        // check value
        Object valueObject = james.value();
        assertTrue(valueObject instanceof Struct);
        Struct value = (Struct) valueObject;
        assertEquals(1, value.get("userid"));
        assertEquals("James", value.get("name"));
    }

    private void expectOffsetLookupReturnNone() {
        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offset(EasyMock.anyObject(Map.class))).andReturn(null);
    }
    
    private void runSql(String statement) throws SQLException {
        connection.createStatement().executeUpdate(statement);
    }
    
    private List<SourceRecord> pollUntilRows() throws InterruptedException {
        List<SourceRecord> records = null;
        while (true) {
            records = task.poll();
            if (records != null) {
                break;
            } else {
                System.out.println("null from poll");
            }
        }
        return records;
    }
}

