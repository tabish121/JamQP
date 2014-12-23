package org.apache.activemq.jamqp;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.jamqp.sasl.SaslAuthenticator;
import org.apache.activemq.jamqp.support.ClientFuture;
import org.apache.activemq.jamqp.support.ClientTcpTransport;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Event.Type;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.CollectorImpl;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpConnection extends AmqpAbstractResource<Connection> implements ClientTcpTransport.TransportListener {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private static final int DEFAULT_MAX_FRAME_SIZE = 1024 * 1024 * 1;
    // NOTE: Limit default channel max to signed short range to deal with
    //       brokers that don't currently handle the unsigned range well.
    private static final int DEFAULT_CHANNEL_MAX = 32767;
    public static final long DEFAULT_CONNECT_TIMEOUT = 15000;
    public static final long DEFAULT_CLOSE_TIMEOUT = 30000;

    private final ScheduledExecutorService serializer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Collector protonCollector = new CollectorImpl();
    private final ClientTcpTransport transport;
    private final Transport protonTransport = Transport.Factory.create();

    private final String username;
    private final String password;
    private final URI remoteURI;

    private AmqpClientListener listener;
    private SaslAuthenticator authenticator;

    private String containerId;
    private boolean connected;
    private boolean authenticated;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private long connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private long closeTimeout = DEFAULT_CLOSE_TIMEOUT;

    public AmqpConnection(ClientTcpTransport transport, String username, String password) {
        super(Connection.Factory.create());

        getEndpoint().collect(protonCollector);

        this.transport = transport;
        this.username = username;
        this.password = password;
        this.remoteURI = transport.getRemoteURI();

        this.serializer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName(toString());
                return serial;
            }
        });

        this.transport.setTransportListener(this);
    }

    protected void connect() throws Exception {
        transport.connect();

        final ClientFuture future = new ClientFuture();
        serializer.execute(new Runnable() {
            @Override
            public void run() {
                getEndpoint().setContainer(safeGetContainerId());
                getEndpoint().setHostname(remoteURI.getHost());

                protonTransport.setMaxFrameSize(getMaxFrameSize());
                protonTransport.setChannelMax(getChannelMax());
                protonTransport.bind(getEndpoint());
                Sasl sasl = protonTransport.sasl();
                if (sasl != null) {
                    sasl.client();
                }
                authenticator = new SaslAuthenticator(sasl, username, password);
                open(future);

                pumpToProtonTransport();
            }
        });

        if (connectTimeout < 0) {
            future.sync();
        } else {
            future.sync(connectTimeout, TimeUnit.MILLISECONDS);
        }
    }

    public boolean isConnected() {
        return transport.isConnected() && connected;
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            final ClientFuture request = new ClientFuture();
            serializer.execute(new Runnable() {

                @Override
                public void run() {
                    try {

                        // If we are not connected then there is nothing we can do now
                        // just signal success.
                        if (!transport.isConnected()) {
                            request.onSuccess();
                        }

                        if (getEndpoint() != null) {
                            close(request);
                        } else {
                            request.onSuccess();
                        }

                        pumpToProtonTransport();
                    } catch (Exception e) {
                        LOG.debug("Caught exception while closing proton connection");
                    }
                }
            });

            try {
                if (closeTimeout < 0) {
                    request.sync();
                } else {
                    request.sync(closeTimeout, TimeUnit.MILLISECONDS);
                }
            } catch (IOException e) {
                LOG.warn("Error caught while closing Provider: ", e.getMessage());
            } finally {
                if (transport != null) {
                    try {
                        transport.close();
                    } catch (Exception e) {
                        LOG.debug("Cuaght exception while closing down Transport: {}", e.getMessage());
                    }
                }

                serializer.shutdown();
            }
        }
    }

    /**
     * Creates a new Session instance used to create AMQP resources like
     * senders and receivers.
     *
     * @return a new AmqpSession that can be used to create links.
     *
     * @throws Exception if an error occurs during creation.
     */
    public AmqpSession createSession() throws Exception {
        checkClosed();

        final ClientFuture request = new ClientFuture();
        serializer.execute(new Runnable() {

            @Override
            public void run() {
                checkClosed();

                AmqpSession session = new AmqpSession(AmqpConnection.this, getEndpoint().session());
                session.open(request);

                pumpToProtonTransport();
            }
        });

        return (AmqpSession) request.sync();
    }

    /**
     * @return the user name that was used to authenticate this connection.
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the password that was used to authenticate this connection.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @return the URI of the remote peer this connection attached to.
     */
    public URI getRemoteURI() {
        return remoteURI;
    }

    /**
     * @return the container ID that will be set as the container Id.
     */
    public String getContainerId() {
        return this.containerId;
    }

    /**
     * Sets the container Id that will be configured on the connection prior to
     * connecting to the remote peer.  Calling this after connect has no effect.
     *
     * @param containerId
     * 		  the container Id to use on the connection.
     */
    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    /**
     * @return the currently set Max Frame Size value.
     */
    public int getMaxFrameSize() {
        return DEFAULT_MAX_FRAME_SIZE;
    }

    public int getChannelMax() {
        return channelMax;
    }

    public void setChannelMax(int channelMax) {
        this.channelMax = channelMax;
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

    protected void fireClientException(Throwable ex) {
        AmqpClientListener listener = this.listener;
        if (listener != null) {
            listener.onClientException(ex);
        }
    }

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("The Connection is already closed");
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
                        AmqpConnection connection = (AmqpConnection) protonEvent.getConnection().getContext();
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

    private void processSaslAuthentication() {
        if (connected || authenticator == null) {
            return;
        }

        try {
            if (authenticator.authenticate()) {
                authenticator = null;
                authenticated = true;
            }
        } catch (SecurityException ex) {
            failed(ex);
        }
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

    private String safeGetContainerId() {
        String containerId = getContainerId();
        if (containerId == null || containerId.isEmpty()) {
            containerId = UUID.randomUUID().toString();
        }

        return containerId;
    }
}
