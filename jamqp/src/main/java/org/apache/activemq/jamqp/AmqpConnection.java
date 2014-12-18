package org.apache.activemq.jamqp;

import java.io.IOException;

import org.apache.activemq.jamqp.support.AsyncResult;

public class AmqpConnection implements AmqpResource {

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

}
