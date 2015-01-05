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

import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton.engine.Receiver;

/**
 * Receiver class that manages a Proton receiver endpoint.
 */
public class AmqpReceiver extends AmqpAbstractResource<Receiver> {

    private final AmqpSession session;
    private final String address;

    /**
     * Create a new receiver instance.
     *
     * @param session
     * 		  The parent session that created the receiver.
     * @param address
     *        The address that this receiver should listen on.
     */
    public AmqpReceiver(AmqpSession session, String address) {
        this.session = session;
        this.address = address;
    }

    /**
     * @return this session's parent AmqpSession.
     */
    public AmqpSession getSession() {
        return session;
    }

    /**
     * @return the address that this receiver has been configured to listen on.
     */
    public String getAddress() {
        return address;
    }

    /**
     * Attempts to wait on a message to be delivered to this receiver.  The receive
     * call will wait indefinitely for a message to be delivered.
     *
     * @return a newly received message sent to this receiver.
     *
     * @throws Exception if an error occurs during the receive attempt.
     */
    public AmqpMessage receive() throws Exception {
        return null;
    }

    /**
     * Attempts to receive a message sent to this receiver, waiting for the given
     * timeout value before giving up and returning null.
     *
     * @param timeout
     * 	      the time to wait for a new message to arrive.
     * @param unit
     * 		  the unit of time that the timeout value represents.
     *
     * @return a newly received message or null if the time to wait period expires.
     *
     * @throws Exception if an error occurs during the receive attempt.
     */
    public AmqpMessage receive(long timeout, TimeUnit unit) throws Exception {
        return null;
    }

    /**
     * If a message is already available in this receiver's prefetch buffer then
     * it is returned immediately otherwise this methods return null without waiting.
     *
     * @return a newly received message or null if there is no currently available message.
     *
     * @throws Exception if an error occurs during the receive attempt.
     */
    public AmqpMessage receiveNoWait() throws Exception {
        return null;
    }
}
