/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.wushujames.connect.mysql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaBuilder;
import org.apache.kafka.copycat.data.Struct;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.source.SourceRecord;
import org.apache.kafka.copycat.source.SourceTask;
import org.apache.kafka.copycat.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellLogging;
import com.zendesk.maxwell.MaxwellMysqlStatus;
import com.zendesk.maxwell.MaxwellReplicator;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;


/**
 * MySqlSourceTask reads from stdin or a file.
 */
public class MySqlSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MySqlSourceTask.class);
    public static final String FILENAME_FIELD = "filename";
    public  static final String POSITION_FIELD = "position";
    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private com.zendesk.maxwell.schema.Schema schema;
    private MaxwellConfig config;
    private MaxwellContext maxwellContext;
    private MaxwellReplicator replicator;
    static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

    
    @Override
    public void start(Properties props) {
        try {
            OffsetStorageReader offsetStorageReader = this.context.offsetStorageReader();
            Map<String, Object> offsetFromCopycat = offsetStorageReader.offset(sourcePartition());
            
            // XXX Need an API to pass values to Maxwell. For now, do it the dumb
            // way.
            String[] argv = new String[] {
                    "--user=" + props.getProperty(MySqlSourceConnector.USER_CONFIG), 
                    "--password=" + props.getProperty(MySqlSourceConnector.PASSWORD_CONFIG), 
                    "--host=" + props.getProperty(MySqlSourceConnector.HOST_CONFIG),
                    "--port=" + props.getProperty(MySqlSourceConnector.PORT_CONFIG)
            };
            this.config = new MaxwellConfig(argv);

            if ( this.config.log_level != null )
                MaxwellLogging.setLevel(this.config.log_level);

            this.maxwellContext = new MaxwellContext(this.config);
            BinlogPosition startAt;
            
            try ( Connection connection = this.maxwellContext.getConnectionPool().getConnection() ) {
                MaxwellMysqlStatus.ensureMysqlState(connection);

                SchemaStore.ensureMaxwellSchema(connection);
                SchemaStore.upgradeSchemaStoreSchema(connection);

                SchemaStore.handleMasterChange(connection, maxwellContext.getServerID());

                if ( offsetFromCopycat != null) {
                    System.out.println("have copycat offsets! " + offsetFromCopycat);
                    startAt = new BinlogPosition((long) offsetFromCopycat.get(POSITION_FIELD),
                            (String) offsetFromCopycat.get(FILENAME_FIELD));
                    LOGGER.info("Maxwell is booting, starting at " + startAt);
                    SchemaStore store = SchemaStore.restore(connection, this.maxwellContext.getServerID(), startAt);
                    this.schema = store.getSchema();
                } else {
                    System.out.println("no copycat offsets!");
                    LOGGER.info("Maxwell is capturing initial schema");
                    SchemaCapturer capturer = new SchemaCapturer(connection);
                    this.schema = capturer.capture();
                    
                    startAt = BinlogPosition.capture(connection);
                    SchemaStore store = new SchemaStore(connection, this.maxwellContext.getServerID(), this.schema, startAt);
                    store.save();

                }
            } catch ( SQLException e ) {
                LOGGER.error("Failed to connect to mysql server @ " + this.config.getConnectionURI());
                LOGGER.error(e.getLocalizedMessage());
                throw e;
            }
            
            // TODO Auto-generated method stub
            this.replicator = new MaxwellReplicator(this.schema, null /* producer */, this.maxwellContext, startAt);
            this.maxwellContext.start();

            this.replicator.beforeStart(); // starts open replicator
            
        } catch (Exception e) {
            throw new CopycatException("Error Initializing Maxwell", e);
        }
    }

    private void initFirstRun(Connection connection) throws SQLException, IOException, SchemaSyncError {
        LOGGER.info("Maxwell is capturing initial schema");
        SchemaCapturer capturer = new SchemaCapturer(connection);
        this.schema = capturer.capture();

        BinlogPosition pos = BinlogPosition.capture(connection);
        SchemaStore store = new SchemaStore(connection, this.maxwellContext.getServerID(), this.schema, pos);
        store.save();

        this.maxwellContext.setPosition(pos);
    }


    
    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        try {
            MaxwellAbstractRowsEvent event = replicator.getEvent();
            this.maxwellContext.ensurePositionThread();

            if (event == null) {
                return null;
            }

            if (event.getTable().getDatabase().getName().equals("maxwell")) {
                return null;
            }

            String databaseName = event.getDatabase().getName();
            String tableName = event.getTable().getName();
            
            String topicName = databaseName + "." + tableName;
            
            ArrayList<SourceRecord> records = new ArrayList<>();

            Table table = event.getTable();
            
            List<Row> rows = event.filteredRows();
            // databaseName.tableName
            SchemaBuilder pkBuilder = SchemaBuilder.struct().name(databaseName + "." + tableName);

            // create schema for primary key
            for (String pk : table.getPKList()) {
                int idx = table.findColumnIndex(pk);
                ColumnDef def = table.getColumnList().get(idx);
                
                switch (def.getType()) {
                case "bool":
                case "boolean":
                    pkBuilder.field(pk, Schema.BOOLEAN_SCHEMA);
                    break;
                case "bit":
                case "tinyint":
                    pkBuilder.field(pk, Schema.INT8_SCHEMA);
                    break;
                case "smallint":
                    pkBuilder.field(pk, Schema.INT16_SCHEMA);
                    break;
                case "mediumint":
                case "int":
                    pkBuilder.field(pk, Schema.INT32_SCHEMA);
                    break;
                default:
                    throw new RuntimeException("unsupported type");
                }
            }
            Schema pkSchema = pkBuilder.build();
            
            List<Struct> primaryKeys = new ArrayList<Struct>();
            
            for (Row row : rows) {
                // make primary key schema
                Struct pkStruct = new Struct(pkSchema);

                for (String pk : table.getPKList()) {
                    int idx = table.findColumnIndex(pk);
                    
                    Column column = row.getColumns().get(idx);
                    ColumnDef def = table.getColumnList().get(idx);

                    // add to my key structure
                    Long l = (Long) def.asJSON(column.getValue());
                    pkStruct.put(pk, l.intValue());
                }
                primaryKeys.add(pkStruct);
            }
            
            Iterator<String> jsonIter = event.toJSONStrings().iterator();
//            Iterator<String> keysIter = event.getPKStrings().iterator();

            Iterator<Struct> pkIter = primaryKeys.iterator();
            
            while (jsonIter.hasNext() && pkIter.hasNext()) {
                String json = jsonIter.next();
                Struct key = pkIter.next();

                System.out.print("got a maxwell event!");
                System.out.println(json);
                SourceRecord rec = new SourceRecord(
                        sourcePartition(), 
                        sourceOffset(event),
                        topicName, 
                        null, //partition 
                        pkSchema,
                        key,
                        VALUE_SCHEMA, 
                        json);
                records.add(rec);
            }
            return records;

            //return records;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public void stop() {
        log.trace("Stopping");
        try {
            this.replicator.beforeStop();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private Map<String, String> sourcePartition() {
        return Collections.singletonMap("host", "192.168.59.103");
    }

    private Map<String, Object> sourceOffset(MaxwellAbstractRowsEvent event) {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put(POSITION_FIELD, event.getHeader().getNextPosition());
        m.put(FILENAME_FIELD, event.getBinlogFilename());
        
        return m;
    }
}
