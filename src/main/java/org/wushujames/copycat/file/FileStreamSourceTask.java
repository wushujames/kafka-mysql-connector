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

package org.wushujames.copycat.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.source.SourceRecord;
import org.apache.kafka.copycat.source.SourceTask;
import org.apache.kafka.copycat.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;


/**
 * FileStreamSourceTask reads from stdin or a file.
 */
public class FileStreamSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FileStreamSourceTask.class);
    public static final String FILENAME_FIELD = "filename";
    public  static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private InputStream stream;
    private com.zendesk.maxwell.schema.Schema schema;
    private MaxwellConfig config;
    private MaxwellContext maxwellContext;
    private MaxwellReplicator replicator;
    static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

    
    @Override
    public void start(Properties props) {
        try {
            OffsetStorageReader offsetStorageReader = this.context.offsetStorageReader();
            Map<String, Object> offsetFromCopycat = offsetStorageReader.offset(offsetKey());
            
            // XXX Need an API to pass values to Maxwell. For now, do it the dumb
            // way.
            String[] argv = new String[] {
                    "--user=" + props.getProperty(FileStreamSourceConnector.USER_CONFIG), 
                    "--password=" + props.getProperty(FileStreamSourceConnector.PASSWORD_CONFIG), 
                    "--host=" + props.getProperty(FileStreamSourceConnector.HOST_CONFIG),
                    "--port=" + props.getProperty(FileStreamSourceConnector.PORT_CONFIG)
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
            SourceRecord rec;
            for (String json : event.toJSONStrings()) {
                System.out.print("got a maxwell event!");
                System.out.println(json);
                rec = new SourceRecord(
                        offsetKey(), 
                        offsetValue(event),
                        topicName, 
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

    private Map<String, String> offsetKey() {
        return Collections.singletonMap("host", "192.168.59.103");
    }

    private Map<String, Object> offsetValue(MaxwellAbstractRowsEvent event) {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put(POSITION_FIELD, event.getHeader().getNextPosition());
        m.put(FILENAME_FIELD, event.getBinlogFilename());
        
        return m;
    }
}
