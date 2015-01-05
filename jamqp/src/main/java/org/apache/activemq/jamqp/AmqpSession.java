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

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.jamqp.support.ClientFuture;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Session;

/**
 * Session class that manages a Proton session endpoint.
 */
public class AmqpSession extends AmqpAbstractResource<Session> {

    private final AmqpConnection connection;

    /**
     * Create a new session instance.
     *
     * @param connection
     * 		  The parent connection that created the session.
     * @param session
     *        The proton session that will be managed by this class.
     */
    public AmqpSession(AmqpConnection connection) {
        this.connection = connection;
    }

    /**
     * Create a sender instance using the given address
     *
     * @param address
     * 	      the address to which the sender will produce its messages.
     *
     * @return a newly created sender that is ready for use.
     *
     * @throws Exception if an error occurs while creating the sender.
     */
    public AmqpSender createSender(final String address) throws Exception {
        checkClosed();

        final AmqpSender sender = new AmqpSender(AmqpSession.this, address);
        final ClientFuture request = new ClientFuture();

        connection.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();

                // TODO - Better name;
                sender.setEndpoint(getEndpoint().sender(UUID.randomUUID().toString()));
                sender.open(request);

                pumpToProtonTransport();
            }
        });

        request.sync();

        return sender;
    }

    /**
     * Create a receiver instance using the given address
     *
     * @param address
     * 	      the address to which the receiver will subscribe for its messages.
     *
     * @return a newly created receiver that is ready for use.
     *
     * @throws Exception if an error occurs while creating the receiver.
     */
    public AmqpReceiver createReceiver(String address) throws Exception {
        checkClosed();

        final AmqpReceiver receiver = new AmqpReceiver(AmqpSession.this, address);
        final ClientFuture request = new ClientFuture();

        connection.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();

                // TODO - Better name;
                receiver.setEndpoint(getEndpoint().receiver(UUID.randomUUID().toString()));
                receiver.open(request);

                pumpToProtonTransport();
            }
        });

        request.sync();

        return receiver;
    }

    /**
     * @return this session's parent AmqpConnection.
     */
    public AmqpConnection getConnection() {
        return connection;
    }

    //----- Internal getters used from the child AmqpResource classes --------//

    ScheduledExecutorService getScheduler() {
        return connection.getScheduler();
    }

    Connection getProtonConnection() {
        return connection.getProtonConnection();
    }

    void pumpToProtonTransport() {
        connection.pumpToProtonTransport();
    }

    //----- Private implementation details -----------------------------------//

    private void checkClosed() {
        if (isClosed()) {
            throw new IllegalStateException("Session is already closed");
        }
    }
}
