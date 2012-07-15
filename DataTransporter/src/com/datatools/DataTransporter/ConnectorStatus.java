package com.datatools.DataTransporter;

/**
 * 
 * @author Vinod Panicker
 * @version 1.0
 * 
 * All Possible Connector Status
 *
 */

public enum ConnectorStatus {

	//TODO will have to refactor all fail conditions to _FAILED in the end or via-versa in ConnectionStatus
	PAUSE, CONNECTED, PRE_INIT_VALIDATION_DONE, READ_COMPLETED, WRITE_COMPLETED, CLEANUP_DONE, DISCONNECTED, STOPPED, STARTED, NOT_ACTIVATED, FAILED_TO_CONNECT, FAILED_VALIDATION,FAILED_TO_WRITE, FAILED_TO_READ, ACTIVATED, STOPPING, PRE_INIT_VALIDATION_FAILED, STOP, DEACTIVATED, PAUSED, READING, WRITING;


    /**
     * Checks if the connector has failed 
     * @param status
     * @return
     */
	public static boolean hasFailed(ConnectorStatus status) {
		
		return status==FAILED_TO_CONNECT||status==PRE_INIT_VALIDATION_FAILED||status==FAILED_VALIDATION||status==FAILED_TO_WRITE||status==FAILED_TO_READ;
	}

	public static boolean isBeingActivated(ConnectorStatus status) {
		
		return status==CONNECTED||status==PRE_INIT_VALIDATION_DONE;
	}

	public static boolean hasBeenActivated(ConnectorStatus status) {
		return status==ACTIVATED;
	}

	public static boolean hasBeenDeActivated(ConnectorStatus status) {
		
		return status==DEACTIVATED;
	}

	public static boolean isBeingDeActivated(ConnectorStatus status) {
		
//		return status==STOPPING||status==CLEANUP_DONE||status==DISCONNECTED;
		return status==STOP;
	}

	public static boolean hasPaused(ConnectorStatus status) {
		
		return status==PAUSED;
	}

	public static boolean isActive(ConnectorStatus status) {
		
		return status==READING||status==WRITING;
	}

	public static boolean mustStop(ConnectorStatus status) {
		
		return status==STOP;
	}
}