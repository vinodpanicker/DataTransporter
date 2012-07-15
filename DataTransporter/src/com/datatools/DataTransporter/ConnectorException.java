package com.datatools.DataTransporter;

public class ConnectorException extends Exception {
	
	public ConnectorException(String connectionName)
	{
		ConnectionManager.setConnectionStatus(connectionName, ConnectionStatus.CONNECTOR_FAILED);
	}

}