/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.jamqp.tests.support;

import java.net.URI;
import java.util.Map;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.jamqp.AmqpClient;
import org.apache.activemq.jamqp.AmqpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JamQPTestSupport extends ActiveMQTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JamQPTestSupport.class);

    protected boolean isAmqpDiscovery() {
        return false;
    }

    protected String getAmqpTransformer() {
        return "jms";
    }

    protected int getSocketBufferSize() {
        return 64 * 1024;
    }

    protected int getIOBufferSize() {
        return 8 * 1024;
    }

    @Override
    protected void addAdditionalConnectors(BrokerService brokerService, Map<String, Integer> portMap) throws Exception {
        int port = 0;
        if (portMap.containsKey("amqp")) {
            port = portMap.get("amqp");
        }
        TransportConnector connector = brokerService.addConnector(
            "amqp://0.0.0.0:" + port + "?transport.transformer=" + getAmqpTransformer() +
            "&transport.socketBufferSize=" + getSocketBufferSize() + "&ioBufferSize=" + getIOBufferSize());
        connector.setName("amqp");
        if (isAmqpDiscovery()) {
            connector.setDiscoveryUri(new URI("multicast://default"));
        }
        port = connector.getPublishableConnectURI().getPort();
        LOG.debug("Using amqp port: {}", port);
    }

    public String getAmqpConnectionURIOptions() {
        return "";
    }

    public URI getBrokerAmqpConnectionURI() {
        try {
            String uri = "tcp://127.0.0.1:" +
                brokerService.getTransportConnectorByName("amqp").getPublishableConnectURI().getPort();

            if (!getAmqpConnectionURIOptions().isEmpty()) {
                uri = uri + "?" + getAmqpConnectionURIOptions();
            }

            return new URI(uri);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public AmqpConnection createAmqpConnection() throws Exception {
        return createAmqpConnection(getBrokerAmqpConnectionURI());
    }

    public AmqpConnection createAmqpConnection(String username, String password) throws Exception {
        return createAmqpConnection(getBrokerAmqpConnectionURI(), username, password);
    }

    public AmqpConnection createAmqpConnection(URI brokerURI) throws Exception {
        return createAmqpConnection(brokerURI, null, null);
    }

    public AmqpConnection createAmqpConnection(URI brokerURI, String username, String password) throws Exception {
        return createAmqpClient(brokerURI, username, password).connect();
    }

    public AmqpClient createAmqpClient() throws Exception {
        return createAmqpClient(getBrokerAmqpConnectionURI(), null, null);
    }

    public AmqpClient createAmqpClient(URI brokerURI) throws Exception {
        return createAmqpClient(brokerURI, null, null);
    }

    public AmqpClient createAmqpClient(String username, String password) throws Exception {
        return createAmqpClient(getBrokerAmqpConnectionURI(), username, password);
    }

    public AmqpClient createAmqpClient(URI brokerURI, String username, String password) throws Exception {
        return new AmqpClient(brokerURI, username, password);
    }
}