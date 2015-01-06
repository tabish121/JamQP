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
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.activemq.jamqp.support.AsyncResult;
import org.apache.activemq.jamqp.support.ClientFuture;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sender class that manages a Proton sender endpoint.
 */
public class AmqpSender extends AmqpAbstractResource<Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpSender.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    private final AmqpTransferTagGenerator tagGenerator = new AmqpTransferTagGenerator(true);

    private final AmqpSession session;
    private final String address;

    private boolean presettle;

    private final Set<Delivery> pending = new LinkedHashSet<Delivery>();
    private byte[] encodeBuffer = new byte[1024 * 8];

    /**
     * Create a new sender instance.
     *
     * @param session
     * 		  The parent session that created the session.
     * @param sender
     *        The proton sender that will be managed by this class.
     */
    public AmqpSender(AmqpSession session, String address) {
        this.session = session;
        this.address = address;
    }

    /**
     * Sends the given message to this senders assigned address.
     *
     * @param message
     *        the message to send.
     *
     * @throws IOException if an error occurs during the send.
     */
    public void send(final AmqpMessage message) throws IOException {
        final ClientFuture sendRequest = new ClientFuture();

        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                try {
                    doSend(message, sendRequest);
                } catch (Exception e) {
                    session.getConnection().fireClientException(e);
                }
            }
        });

        // TODO - Send timeouts
        sendRequest.sync();
    }

    /**
     * @return this session's parent AmqpSession.
     */
    public AmqpSession getSession() {
        return session;
    }

    /**
     * @return the assigned address of this sender.
     */
    public String getAddress() {
        return address;
    }

    //----- Sender configuration ---------------------------------------------//

    /**
     * @return will messages be settle on send.
     */
    public boolean isPresettle() {
        return presettle;
    }

    /**
     * Configure is sent messages are marked as settled on send, defaults to false.
     *
     * @param presettle
     * 		  configure if this sender will presettle all sent messages.
     */
    public void setPresettle(boolean presettle) {
        this.presettle = presettle;
    }

    //----- Private Sender implementation ------------------------------------//

    private void doSend(AmqpMessage message, AsyncResult request) throws Exception {

        LOG.trace("Producer sending message: {}", message);

        byte[] tag = tagGenerator.getNextTag();
        Delivery delivery = null;

        if (presettle) {
            delivery = getEndpoint().delivery(EMPTY_BYTE_ARRAY, 0, 0);
        } else {
            delivery = getEndpoint().delivery(tag, 0, tag.length);
        }

        delivery.setContext(request);

        encodeAndSend(message.getWrappedMessage(), delivery);

        if (presettle) {
            delivery.settle();
            request.onSuccess();
        } else {
            pending.add(delivery);
            getEndpoint().advance();
        }
    }

    private void encodeAndSend(Message message, Delivery delivery) throws IOException {

        int encodedSize;
        while (true) {
            try {
                encodedSize = message.encode(encodeBuffer, 0, encodeBuffer.length);
                break;
            } catch (java.nio.BufferOverflowException e) {
                encodeBuffer = new byte[encodeBuffer.length * 2];
            }
        }

        int sentSoFar = 0;

        while (true) {
            int sent = getEndpoint().send(encodeBuffer, sentSoFar, encodedSize - sentSoFar);
            if (sent > 0) {
                sentSoFar += sent;
                if ((encodedSize - sentSoFar) == 0) {
                    break;
                }
            } else {
                LOG.warn("{} failed to send any data from current Message.", this);
            }
        }
    }
}
