package org.apache.activemq.jamqp;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.jamqp.support.AsyncResult;
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

public class AmqpConnection implements ClientTcpTransport.TransportListener, AmqpResource {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private final ScheduledExecutorService serializer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Collector protonCollector = new CollectorImpl();
    private final ClientTcpTransport transport;

    private final String username;
    private final String password;
    private final URI remoteURI;

    private AmqpClientListener listener;
    private Transport protonTransport;
    private Connection protonConnection;

    private boolean authenticated;

    public AmqpConnection(ClientTcpTransport transport, String username, String password) {

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
    }

    public boolean isConnected() {
        return transport.isConnected();
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {

            if (!transport.isConnected()) {
                return;
            }

            if (protonConnection != null) {
                protonConnection.close();
                pumpToProtonTransport();
            }

            transport.close();
        }
    }

    @Override
    public void open(AsyncResult request) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isOpen() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isAwaitingOpen() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void opened() {
        // TODO Auto-generated method stub

    }

    @Override
    public void close(AsyncResult request) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isAwaitingClose() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void closed() {
        // TODO Auto-generated method stub

    }

    @Override
    public void failed() {
        // TODO Auto-generated method stub

    }

    @Override
    public void remotelyClosed() {
        // TODO Auto-generated method stub

    }

    @Override
    public void failed(Exception cause) {
        // TODO Auto-generated method stub

    }

    @Override
    public void processStateChange() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void processDeliveryUpdates() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void processFlowUpdates() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean hasRemoteError() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Exception getRemoteError() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getRemoteErrorMessage() {
        // TODO Auto-generated method stub
        return null;
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

    /**
     *
     */
    private void processSaslAuthentication() {
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
