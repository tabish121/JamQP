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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.jamqp.support.ClientTcpTransport;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Event.Type;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.CollectorImpl;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection instance used to connect to the Broker using Proton as
 * the AMQP protocol handler.
 */
public class AmqpClient implements ClientTcpTransport.TransportListener {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpClient.class);

    private final ScheduledExecutorService serializer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Collector protonCollector = new CollectorImpl();
    private final AmqpClientListener listener;

    private ClientTcpTransport transport;
    private Transport protonTransport;
    private Connection protonConnection;

    private String username;
    private String password;
    private URI remoteURI;
    private boolean authenticated;

    public AmqpClient(AmqpClientListener listener) {
        this.listener = listener;

        this.serializer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName(toString());
                return serial;
            }
        });
    }

    /**
     * Creates an anonymous connection with the broker at the given location.
     *
     * @param broker
     *        the address of the remote broker instance.
     *
     * @throws Exception if an error occurs attempting to connect to the Broker.
     */
    public void connect(URI broker) throws Exception {
        connect(broker, null, null);
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
     * @throws Exception if an error occurs attempting to connect to the Broker.
     */
    public void connect(URI broker, String username, String password) throws Exception {
        if (username == null && password != null) {
            throw new IllegalArgumentException("Password must be null if user name value is null");
        }

        if (broker.getScheme().equals("tcp")) {

        } else {
            throw new IllegalArgumentException("Client only support TCP currently.");
        }
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {

            if (!transport.isConnected()) {
                return;
            }

            protonConnection.close();
            pumpToProtonTransport();
            transport.close();
        }
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
    public void onData(final Buffer input) {
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                ByteBuffer source = input.toByteBuffer();
                LOG.trace("Received from Broker {} bytes:", source.remaining());

                do {
                    ByteBuffer buffer = protonTransport.getInputBuffer();
                    int limit = Math.min(buffer.remaining(), source.remaining());
                    ByteBuffer duplicate = source.duplicate();
                    duplicate.limit(source.position() + limit);
                    buffer.put(duplicate);
                    protonTransport.processInput();
                    source.position(source.position() + limit);
                } while (source.hasRemaining());

                // Process the state changes from the latest data and then answer back
                // any pending updates to the Broker.
                processUpdates();
                pumpToProtonTransport();
            }
        });
    }

    @Override
    public void onTransportClosed() {
        LOG.debug("The transport has unexpectedly closed");
    }

    @Override
    public void onTransportError(Throwable cause) {
        fireClientException(cause);
    }

    @Override
    public String toString() {
        return "AmqpClient: " + getRemoteURI().getHost() + ":" + getRemoteURI().getPort();
    }

    protected void fireClientException(Throwable ex) {
        AmqpClientListener listener = this.listener;
        if (listener != null) {
            listener.onClientException(ex);
        }
    }

    private void processUpdates() {
        try {
            Event protonEvent = null;
            while ((protonEvent = protonCollector.peek()) != null) {
                if (!protonEvent.getType().equals(Type.TRANSPORT)) {
                    LOG.trace("New Proton Event: {}", protonEvent.getType());
                }

                AmqpResource amqpResource = null;
                switch (protonEvent.getType()) {
                    case CONNECTION_REMOTE_CLOSE:
                    case CONNECTION_REMOTE_OPEN:
                        AmqpClient connection = (AmqpClient) protonEvent.getConnection().getContext();
                        connection.processStateChange();
                        break;
                    case SESSION_REMOTE_CLOSE:
                    case SESSION_REMOTE_OPEN:
                        AmqpSession session = (AmqpSession) protonEvent.getSession().getContext();
                        session.processStateChange();
                        break;
                    case LINK_REMOTE_CLOSE:
                        LOG.info("Link closed: {}", protonEvent.getLink().getContext());
                        AmqpResource cloedResource = (AmqpResource) protonEvent.getLink().getContext();
                        cloedResource.processStateChange();
                        break;
                    case LINK_REMOTE_DETACH:
                        LOG.info("Link detach: {}", protonEvent.getLink().getContext());
                        AmqpResource detachedResource = (AmqpResource) protonEvent.getLink().getContext();
                        detachedResource.processStateChange();
                        break;
                    case LINK_REMOTE_OPEN:
                        AmqpResource resource = (AmqpResource) protonEvent.getLink().getContext();
                        resource.processStateChange();
                        break;
                    case LINK_FLOW:
                        amqpResource = (AmqpResource) protonEvent.getLink().getContext();
                        amqpResource.processFlowUpdates();
                        break;
                    case DELIVERY:
                        amqpResource = (AmqpResource) protonEvent.getLink().getContext();
                        amqpResource.processDeliveryUpdates();
                        break;
                    default:
                        break;
                }

                protonCollector.pop();
            }

            // We have to do this to pump SASL bytes in as SASL is not event driven yet.
            if (!authenticated) {
                processSaslAuthentication();
            }
        } catch (Exception ex) {
            LOG.warn("Caught Exception during update processing: {}", ex.getMessage(), ex);
            fireClientException(ex);
        }
    }

    /**
     *
     */
    private void processSaslAuthentication() {
        // TODO Auto-generated method stub
    }

    /**
     *
     */
    private void processStateChange() {
        // TODO Auto-generated method stub
    }

    private void pumpToProtonTransport() {
        try {
            boolean done = false;
            while (!done) {
                ByteBuffer toWrite = protonTransport.getOutputBuffer();
                if (toWrite != null && toWrite.hasRemaining()) {
                    transport.send(toWrite);
                    protonTransport.outputConsumed();
                } else {
                    done = true;
                }
            }
        } catch (IOException e) {
            fireClientException(e);
        }
    }
}
