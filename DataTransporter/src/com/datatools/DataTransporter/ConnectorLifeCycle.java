package com.datatools.DataTransporter;

import java.util.ArrayList;

/**
 * 
 * All connectors would implement ConnectorLifeCycle
 * All connectors should extend Connector class
 * 
 * 
 *          The Happy Path for Connector class 
 *          <PRE>State 1)CONNECTOR_NOT_ACTIVATED ,Fail status: CONNECTOR_NOT_FOUND
 *          <PRE>State 2)CONNECTED , Fail status: CONNECTION_FAILED 
 *          <PRE>State 3)PRE_INIT_VALIDATION_DONE , Fail status:PRE_INIT_VALIDATION_FAILED 
 *          <PRE>State 4)CONNECTOR_ACTIVATED ,Fail status :CONNECTOR_ACTIVATION_FAILED 
 *          <PRE>State 5)READ_COMPLETED , Fail status:READ_FAILED 
 *          <PRE>State 6)WRITE_COMPLETED, Fail status:WRITE_FAILED
 *          <PRE>State 7)CLEANUP_DONE, Fail status:CLEANUP_FAILED 
 *          <PRE>State 8)DISCONNECTED,Fail status:CANNOT_DISCONNECT 
 *          <PRE>State 9)CONNECTOR_DEACTIVATED,Fail status: CONNECTOR_DEACTIVATION_FAILED
 *          
 * 
 *          Signal status are 
 *          <PRE>CONNECTOR_STOP
 *          <PRE>CONNECTOR_START
 *          <PRE>CONNECTOR_PAUSE
 * 
 * @author Vinod Panicker
 *
 */
public interface ConnectorLifeCycle {

	// this method will activate connectors based on connection configuration
	public abstract void activate();

	/**
	 * This method will start the de-activation of a connectors
	 */
	public abstract void deactivate();

	/**
	 * Method that is implemented by respective connectors to connect to system
	 * files, database ,systems, repository etc that the specific to the
	 * Connector.
	 */

	public abstract void connect();

	/**
	 * Method implement by respective connectors to disconnect from the
	 * connected resource required for the operations of the connector.
	 */
	public abstract void disconnect();

	/**
	 * Method to be implemented by connectors that have to clean up metadata
	 * from data store or from systems/resources that it is connected too.
	 */
	public abstract void preInitValidation();

	/**
	 * Method implemented by respective connectors to read from the input
	 * resources associated to the connector
	 * @throws ConnectorException 
	 */
	public abstract ArrayList read() throws ConnectorException;

	/**
	 * Method implemented by respective Connectors to write out to the output
	 * resources.
	 * 
	 * @param recordsToWrite
	 * @throws ConnectorException 
	 */
	public abstract void write(ArrayList recordsToWrite) throws ConnectorException;

}