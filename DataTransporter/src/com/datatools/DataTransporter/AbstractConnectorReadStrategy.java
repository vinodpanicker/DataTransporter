package com.datatools.DataTransporter;

public abstract class AbstractConnectorReadStrategy implements ConnectorReadStrategy {
	  
	Connector connector=null; 
	
	public AbstractConnectorReadStrategy(Connector connector) {
		this.connector = connector;
	}

	@Override
	public	abstract void doReadAndTransform() throws ConnectorException;

}