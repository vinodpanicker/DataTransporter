package com.datatools.DataTransporter;

public class ConnectionRecoveryAgent {

	/**
	 * The recovery Agent saves data that is pull in to the connection but not 
	 * pushed to the push connectors, if the agent saves the data it will return
	 * ConnectionStatus.SAVED_CONNECTION_DATA 
	 * 
	 * @param connectionName
	 * @param data
	 * @return
	 */
	public static ConnectionStatus save(String connectionName, Data data) {
		// TODO Auto-generated method stub
		return ConnectionStatus.SAVED_CONNECTION_DATA;
	}

	/**
	 * This method recovers and records into data from the connection recovery file
	 * The method also returns the ConnectionStatus.RECOVERED_PREVIOUS_RUN_DATA
	 * It returns ConnectionStatus.RECOVERED_PREVIOUS_RUN_DATA_FAILED if recovery agent fails to recover
	 * @param connectionName
	 * @param data
	 * @return
	 */
	public static ConnectionStatus recover(String connectionName, Data data) {
		// TODO Auto-generated method stub
		return ConnectionStatus.RECOVERED_PREVIOUS_RUN_DATA;
	}

}