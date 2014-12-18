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
package org.apache.activemq.jamqp;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection instance used to connect to the Broker using Proton as
 * the AMQP protocol handler.
 */
public class AmqpClient {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpClient.class);

    private String username;
    private String password;
    private URI remoteURI;

    /**
     * Creates an AmqpClient instance which can be used as a factory for connections.
     *
     * @param remoteURI
     *        The address of the remote peer to connect to.
     * @param username
     *	      The user name to use when authenticating the client.
     * @param password
     *		  The password to use when authenticating the client.
     */
    public AmqpClient(URI remoteURI, String username, String password) {
        this.remoteURI = remoteURI;
        this.password = password;
        this.username = username;
    }

    /**
     * Creates a connection with the broker at the given location.
     *
     * @param broker
     *        the address of the remote broker instance.
     * @param username
     *        the user name to use to connect to the broker or null for anonymous.
     * @param password
     *        the password to use to connect to the broker, must be null if user name is null.
     *
     * @returns a new connection object used to interact with the connected peer.
     *
     * @throws Exception if an error occurs attempting to connect to the Broker.
     */
    public AmqpConnection connect() throws Exception {
        if (username == null && password != null) {
            throw new IllegalArgumentException("Password must be null if user name value is null");
        }

        if (remoteURI.getScheme().equals("tcp")) {
            LOG.info("Creating new connection to peer: {}", remoteURI);
        } else {
            throw new IllegalArgumentException("Client only support TCP currently.");
        }

        return null;
    }

    /**
     * @return the user name value given when connect was called, always null before connect.
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the password value given when connect was called, always null before connect.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @return the currently set address to use to connect to the AMQP peer.
     */
    public URI getRemoteURI() {
        return remoteURI;
    }

    @Override
    public String toString() {
        return "AmqpClient: " + getRemoteURI().getHost() + ":" + getRemoteURI().getPort();
    }

    /**
     * Creates an anonymous connection with the broker at the given location.
     *
     * @param broker
     *        the address of the remote broker instance.
     *
     * @returns a new connection object used to interact with the connected peer.
     *
     * @throws Exception if an error occurs attempting to connect to the Broker.
     */
    public static AmqpConnection connect(URI broker) throws Exception {
        return connect(broker, null, null);
    }

    /**
     * Creates a connection with the broker at the given location.
     *
     * @param broker
     *        the address of the remote broker instance.
     * @param username
     *        the user name to use to connect to the broker or null for anonymous.
     * @param password
     *        the password to use to connect to the broker, must be null if user name is null.
     *
     * @returns a new connection object used to interact with the connected peer.
     *
     * @throws Exception if an error occurs attempting to connect to the Broker.
     */
    public static AmqpConnection connect(URI broker, String username, String password) throws Exception {
        if (username == null && password != null) {
            throw new IllegalArgumentException("Password must be null if user name value is null");
        }

        AmqpClient client = new AmqpClient(broker, username, password);

        return client.connect();
    }
}
