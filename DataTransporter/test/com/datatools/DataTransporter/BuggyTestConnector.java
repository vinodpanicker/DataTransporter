package com.datatools.DataTransporter;

import java.util.ArrayList;



public class BuggyTestConnector extends Connector {
	Logger log=new Logger().getLogger(this.getClass());
	public BuggyTestConnector(String name) {
		super(name);
	}

	@Override
	public void cleanup() {
		log.info(ConnectionManager.getConnectionStatus(getConnectionName()));
		
	}

	@Override
	public void connect() {
		log.info(ConnectionManager.getConnectionStatus(getConnectionName()));
		
	}

	@Override
	public void disconnect() {
		log.info(ConnectionManager.getConnectionStatus(getConnectionName()));
		
	}

	@Override
	public void preInitValidation() {
		log.info(ConnectionManager.getConnectionStatus(getConnectionName()));
		
	}

	@Override
	public ArrayList read() throws ConnectorException {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		throw new ConnectorException(this.getConnectionName());
	}

	@Override
	public void stop() {
		log.info(ConnectionManager.getConnectionStatus(getConnectionName()));
		
	}

	@Override
	public void write(ArrayList recordsToWrite) throws ConnectorException  {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		throw new ConnectorException(this.getConnectionName());
		
	}

}