package com.datatools.DataTransporter;

public abstract class AbstractConnectorWriteStrategy implements ConnectorWriteStrategy{

	Connector connector=null; 

	public AbstractConnectorWriteStrategy(Connector connector) {
		this.connector = connector;
	}

	public abstract void doFilterAndWrite() throws ConnectorException;

}