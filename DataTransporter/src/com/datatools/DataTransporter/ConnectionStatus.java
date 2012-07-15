package com.datatools.DataTransporter;

/**
 * 
 * @author Vinod Panicker
 * @version 1.0
 * 
 * All Possible Connection Status
 *
 */

public enum ConnectionStatus {
	RECOVERED_PREVIOUS_RUN_DATA, CONNECTION_SHUTDOWN, SAVED_CONNECTION_DATA, DEACTIVATED_CONNECTORS, CONNECTORS_ACTIVATED, CONNECTORS_INITIALIZED, CONFIGURATION_LOADED, CONNECTION_NOT_ACTIVATED, CONNECTOR_ACTIVATION_FAILED, RECOVER_LAST_RUN_DATA, CONNECTION_DEACTIVATED, CONNECTOR_DEACTIVATION_FAILED, CONNECTION_NOT_FOUND, CONNECTION_ACTIVATED, CONNECTION_STOP, CONNECTION_PAUSE, CONNECTOR_ACTIVATED, CONNECTOR_DEACTIVATED, CONNECTOR_FAILED;
	/**
	 * Method returns true if Connection has to be paused
	 * @param connectionName
	 * @return
	 */
	public static boolean isPausedOrFailed(String connectionName) {
	
		ConnectionStatus connectionStatus = ConnectionManager
				.getConnectionStatus(connectionName);
		return connectionStatus == CONNECTION_PAUSE
				|| connectionStatus == CONNECTION_NOT_FOUND
				|| connectionStatus ==CONNECTOR_ACTIVATION_FAILED;
	}
	
	/**
	 * Method returns true if Connection has to be stopped,shutdown or deactivated
	 * @param connectionName
	 * @return
	 */
	public static boolean isStopped(String connectionName) {
	
		ConnectionStatus connectionStatus = ConnectionManager
				.getConnectionStatus(connectionName);
		return connectionStatus == CONNECTION_STOP
				|| connectionStatus == CONNECTION_SHUTDOWN
				|| connectionStatus ==CONNECTION_DEACTIVATED;
	}

	/**
	 * Method returns true if Connection is not stopped or not in pause or failed mode
	 * @param connectionName
	 * @return
	 */
	public static boolean isActive(String connectionName) {
	
		ConnectionStatus connectionStatus = ConnectionManager
				.getConnectionStatus(connectionName);
		return !(isStopped(connectionName)||isPausedOrFailed(connectionName)||connectionStatus ==CONNECTION_NOT_ACTIVATED);
	}

	public static boolean hasFailed(ConnectionStatus status) {
		
		return status==CONNECTOR_ACTIVATION_FAILED;
	}
}