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

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Very simple connector that works with the console. This connector supports both source and
 * sink modes via its 'mode' setting.
 */
public class MySqlSourceConnector extends SourceConnector {
    public static final String HOST_CONFIG = "host";
    public static final String USER_CONFIG = "user";
    public static final String PASSWORD_CONFIG = "password";
    public static final String PORT_CONFIG = "port";
    
    private String host;
    private String user;
    private String password;
    private String port;
    

    @Override
    public void start(Map<String, String> props) {
        host = props.get(HOST_CONFIG);
        user = props.get(USER_CONFIG);
        password = props.get(PASSWORD_CONFIG);
        port = props.get(PORT_CONFIG);
        
        if (host == null || host.isEmpty()) {
            throw new ConnectException("MySqlSourceConnector configuration must include 'host' setting");
        }
        if (user == null || user.isEmpty()) {
            throw new ConnectException("MySqlSourceConnector configuration must include 'user' setting");
        }
        if (password == null || password.isEmpty()) {
            throw new ConnectException("MySqlSourceConnector configuration must include 'password' setting");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MySqlSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<>();
        config.put("user", user);
        config.put("password", password);
        config.put("host", host);
        config.put("port", port);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since MySqlSourceConnector has no background monitoring.
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
