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

import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.source.SourceRecord;
import org.apache.kafka.copycat.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellCompatibilityError;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellLogging;
import com.zendesk.maxwell.MaxwellMysqlStatus;
import com.zendesk.maxwell.MaxwellReplicator;
import com.zendesk.maxwell.producer.StdoutProducer;
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

    private String filename;
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private String topic = "test";

    private Long streamOffset;

    private com.zendesk.maxwell.schema.Schema schema;
    private MaxwellConfig config;
    private MaxwellContext context;
    private MaxwellReplicator replicator;
    static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

    
    @Override
    public void start(Properties props) {
//        filename = props.getProperty(FileStreamSourceConnector.FILE_CONFIG);
//        if (filename == null || filename.isEmpty()) {
//            stream = System.in;
//            // Tracking offset for stdin doesn't make sense
//            streamOffset = null;
//            reader = new BufferedReader(new InputStreamReader(stream));
//        }
//        topic = props.getProperty(FileStreamSourceConnector.TOPIC_CONFIG);
//        if (topic == null)
//            throw new CopycatException("ConsoleSourceTask config missing topic setting");

        try {
            String[] argv = new String[] {
                    "--user=maxwell", 
                    "--password=XXXXXX", 
                    "--host=192.168.59.103"
            };
            this.config = new MaxwellConfig(argv);

            if ( this.config.log_level != null )
                MaxwellLogging.setLevel(this.config.log_level);

            this.context = new MaxwellContext(this.config);

            try ( Connection connection = this.context.getConnectionPool().getConnection() ) {
                MaxwellMysqlStatus.ensureMysqlState(connection);

                SchemaStore.ensureMaxwellSchema(connection);
                SchemaStore.upgradeSchemaStoreSchema(connection);

                SchemaStore.handleMasterChange(connection, context.getServerID());

                if ( this.context.getInitialPosition() != null ) {
                    LOGGER.info("Maxwell is booting, starting at " + this.context.getInitialPosition());
                    SchemaStore store = SchemaStore.restore(connection, this.context.getServerID(), this.context.getInitialPosition());
                    this.schema = store.getSchema();
                } else {
                    initFirstRun(connection);
                }
            } catch ( SQLException e ) {
                LOGGER.error("Failed to connect to mysql server @ " + this.config.getConnectionURI());
                LOGGER.error(e.getLocalizedMessage());
                return;
            }
            
            // TODO Auto-generated method stub
            this.replicator = new MaxwellReplicator(this.schema, null /* producer */, this.context, this.context.getInitialPosition());
            this.context.start();

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
        SchemaStore store = new SchemaStore(connection, this.context.getServerID(), this.schema, pos);
        store.save();

        this.context.setPosition(pos);
    }


    
    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        try {
            MaxwellAbstractRowsEvent event = replicator.getEvent();
            this.context.ensurePositionThread();

            if (event == null) {
                return null;
            }

            if (event.getTable().getDatabase().getName().equals("maxwell")) {
                return null;
            }

            ArrayList<SourceRecord> records = new ArrayList<>();
            SourceRecord rec;
            for (String json : event.toJSONStrings()) {
                System.out.print("got a maxwell event!");
                System.out.println(json);
                rec = new SourceRecord(
                        offsetKey("192.168.59.103"), 
                        offsetValue(1L),
                        topic, 
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


    private String extractLine() {
        int until = -1, newStart = -1;
        for (int i = 0; i < offset; i++) {
            if (buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') {
                // We need to check for \r\n, so we must skip this if we can't check the next char
                if (i + 1 >= offset)
                    return null;

                until = i;
                newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
                break;
            }
        }

        if (until != -1) {
            String result = new String(buffer, 0, until);
            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
            offset = offset - newStart;
            if (streamOffset != null)
                streamOffset += newStart;
            return result;
        } else {
            return null;
        }
    }

    @Override
    public void stop() {
        log.trace("Stopping");
        synchronized (this) {
            try {
                stream.close();
                log.trace("Closed input stream");
            } catch (IOException e) {
                log.error("Failed to close ConsoleSourceTask stream: ", e);
            }
            this.notify();
        }
    }

    private Map<String, String> offsetKey(String mysqlServer) {
        return Collections.singletonMap("host", "192.168.59.103");
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }
}
