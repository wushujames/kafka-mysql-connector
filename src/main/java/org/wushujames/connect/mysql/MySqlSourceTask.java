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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;


/**
 * MySqlSourceTask reads from stdin or a file.
 */
public class MySqlSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MySqlSourceTask.class);
    private static final String FILENAME_FIELD = "filename";
    private static final String POSITION_FIELD = "position";

    private com.zendesk.maxwell.schema.Schema schema;
    private MaxwellConfig config;
    private MaxwellContext maxwellContext;
    private MaxwellReplicator replicator;
    static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

    
    @Override
    public void start(Map<String, String> props) {
        try {
            OffsetStorageReader offsetStorageReader = this.context.offsetStorageReader();
            Map<String, Object> offsetFromCopycat = offsetStorageReader.offset(sourcePartition());
            
            // XXX Need an API to pass values to Maxwell. For now, do it the dumb
            // way.
            String[] argv = new String[] {
                    "--user=" + props.get(MySqlSourceConnector.USER_CONFIG), 
                    "--password=" + props.get(MySqlSourceConnector.PASSWORD_CONFIG), 
                    "--host=" + props.get(MySqlSourceConnector.HOST_CONFIG),
                    "--port=" + props.get(MySqlSourceConnector.PORT_CONFIG)
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
            throw new ConnectException("Error Initializing Maxwell", e);
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
        ArrayList<SourceRecord> records = new ArrayList<>();

        MaxwellAbstractRowsEvent event;
        try {
            event = replicator.getEvent();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return records;
        }
        try {
            this.maxwellContext.ensurePositionThread();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return records;
        }

        if (event == null) {
            return records;
        }

        if (event.getTable().getDatabase().getName().equals("maxwell")) {
            return records;
        }

        String databaseName = event.getDatabase().getName();
        String tableName = event.getTable().getName();

        String topicName = databaseName + "." + tableName;


        Table table = event.getTable();

        List<Row> rows = event.filteredRows();
        // databaseName.tableName
        // create schema for primary key
        Schema pkSchema = DataConverter.convertPrimaryKeySchema(table);

        List<Struct> primaryKeys = new ArrayList<Struct>();

        for (Row row : rows) {
            // make primary key schema
            Struct pkStruct = DataConverter.convertPrimaryKeyData(pkSchema, table, row);
            primaryKeys.add(pkStruct);
        }

        Iterator<Row> rowIter = rows.iterator();
        Iterator<Struct> pkIter = primaryKeys.iterator();

        while (rowIter.hasNext() && pkIter.hasNext()) {
            Row row = rowIter.next();

            // create schema
            Schema rowSchema = DataConverter.convertRowSchema(table);

            Struct rowStruct = DataConverter.convertRowData(rowSchema, table, row);


            Struct key = pkIter.next();

            System.out.print("got a maxwell event!");
            System.out.println(row);
            SourceRecord rec = new SourceRecord(
                    sourcePartition(), 
                    sourceOffset(event),
                    topicName, 
                    null, //partition 
                    pkSchema,
                    key,
                    rowSchema,
                    rowStruct);
            records.add(rec);
        }
        return records;

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

    @Override
    public String version() {
        return new MySqlSourceConnector().version();
    }
}
