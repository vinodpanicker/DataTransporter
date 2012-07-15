package com.datatools.DataTransporter;

public class DebugConnection extends Connection {

	public DebugConnection(String connectionName) {
		super(connectionName);
	}

	@Override
	protected ConfigLoader getConfigLoader() {
		return new TestConfigurationLoader();
	}
	
}