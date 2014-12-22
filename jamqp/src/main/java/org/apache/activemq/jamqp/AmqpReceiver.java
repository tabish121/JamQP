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

import org.apache.qpid.proton.engine.Receiver;

/**
 * Receiver class that manages a Proton receiver endpoint.
 */
public class AmqpReceiver extends AmqpAbstractResource<Receiver> {

    private AmqpSession session;

    /**
     * Create a new receiver instance.
     *
     * @param session
     * 		  The parent session that created the receiver.
     * @param receiver
     *        The proton receiver that will be managed by this class.
     */
    public AmqpReceiver(AmqpSession session, Receiver receiver) {
        super(receiver);

        this.session = session;
    }

    /**
     * @return this session's parent AmqpSession.
     */
    public AmqpSession getSession() {
        return session;
    }
}
