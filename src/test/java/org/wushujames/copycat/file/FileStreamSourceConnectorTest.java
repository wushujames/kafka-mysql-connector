///**
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// **/
//package org.wushujames.connect.mysql;
//
//import org.apache.kafka.copycat.connector.ConnectorContext;
//import org.apache.kafka.copycat.errors.CopycatException;
//import org.junit.Before;
//import org.junit.Test;
//import org.powermock.api.easymock.PowerMock;
//
//import java.util.List;
//import java.util.Properties;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNull;
//
//public class FileStreamSourceConnectorTest {
//
//    private static final String SINGLE_TOPIC = "test";
//    private static final String MULTIPLE_TOPICS = "test1,test2";
//    private static final String FILENAME = "/somefilename";
//
//    private MySqlSourceConnector connector;
//    private ConnectorContext ctx;
//    private Properties sourceProperties;
//
//    @Before
//    public void setup() {
//        connector = new MySqlSourceConnector();
//        ctx = PowerMock.createMock(ConnectorContext.class);
//        connector.initialize(ctx);
//
//        sourceProperties = new Properties();
//        sourceProperties.setProperty(MySqlSourceConnector.TOPIC_CONFIG, SINGLE_TOPIC);
//        sourceProperties.setProperty(MySqlSourceConnector.FILE_CONFIG, FILENAME);
//    }
//
//    @Test
//    public void testSourceTasks() {
//        PowerMock.replayAll();
//
//        connector.start(sourceProperties);
//        List<Properties> taskConfigs = connector.taskConfigs(1);
//        assertEquals(1, taskConfigs.size());
//        assertEquals(FILENAME,
//                taskConfigs.get(0).getProperty(MySqlSourceConnector.FILE_CONFIG));
//        assertEquals(SINGLE_TOPIC,
//                taskConfigs.get(0).getProperty(MySqlSourceConnector.TOPIC_CONFIG));
//
//        // Should be able to return fewer than requested #
//        taskConfigs = connector.taskConfigs(2);
//        assertEquals(1, taskConfigs.size());
//        assertEquals(FILENAME,
//                taskConfigs.get(0).getProperty(MySqlSourceConnector.FILE_CONFIG));
//        assertEquals(SINGLE_TOPIC,
//                taskConfigs.get(0).getProperty(MySqlSourceConnector.TOPIC_CONFIG));
//
//        PowerMock.verifyAll();
//    }
//
//    @Test
//    public void testSourceTasksStdin() {
//        PowerMock.replayAll();
//
//        sourceProperties.remove(MySqlSourceConnector.FILE_CONFIG);
//        connector.start(sourceProperties);
//        List<Properties> taskConfigs = connector.taskConfigs(1);
//        assertEquals(1, taskConfigs.size());
//        assertNull(taskConfigs.get(0).getProperty(MySqlSourceConnector.FILE_CONFIG));
//
//        PowerMock.verifyAll();
//    }
//
//    @Test(expected = CopycatException.class)
//    public void testMultipleSourcesInvalid() {
//        sourceProperties.setProperty(MySqlSourceConnector.TOPIC_CONFIG, MULTIPLE_TOPICS);
//        connector.start(sourceProperties);
//    }
//
//    @Test
//    public void testTaskClass() {
//        PowerMock.replayAll();
//
//        connector.start(sourceProperties);
//        assertEquals(MySqlSourceTask.class, connector.taskClass());
//
//        PowerMock.verifyAll();
//    }
//}
