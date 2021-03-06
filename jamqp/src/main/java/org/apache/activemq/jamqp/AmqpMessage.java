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

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpMessage {

    private AmqpReceiver receiver;
    private Message message;
    private Delivery delivery;

    /**
     * Accepts the message marking it as consumed on the remote peer.
     *
     * @throws Exception if an error occurs during the accept.
     */
    public void accept() throws Exception {
        if (receiver == null) {
            throw new IllegalStateException("Can't accept non-received message.");
        }
    }

    /**
     * Rejects the message.
     *
     * @throws Exception if an error occurs during the reject.
     */
    public void reject() throws Exception {
        if (receiver == null) {
            throw new IllegalStateException("Can't reject non-received message.");
        }
    }

    /**
     * @return the AMQP Delivery object linked to a received message.
     */
    public Delivery getWrappedDelivery() {
        return delivery;
    }

    /**
     * @return the AMQP Message that is wrapped by this object.
     */
    public Message getWrappedMessage() {
        return message;
    }

    /**
     * @return the AmqpReceiver that consumed this message.
     */
    public AmqpReceiver getAmqpReceiver() {
        return receiver;
    }
}
