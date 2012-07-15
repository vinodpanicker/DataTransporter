package com.datatools.DataTransporter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ConnectionManager {
	
	static Map<String, Connection> connections=Collections.synchronizedMap(new HashMap<String, Connection>());
	/**
	 * Method that returns a Connection instance
	 * @param connectionName
	 * @return
	 */
	public static Connection getConnection(String connectionName) {
		
		return getConnection(connectionName,new ArrayList());
	}

	/**
	 * This method returns a connection object if a valid config file is present
	 * @param connectionName
	 * @param input
	 * @return
	 */
	public static Connection getConnection(String connectionName,Object input) {
		
		if(connectionName=="")
			return null;
		
		if(connections.containsKey(connectionName))
			{
			return connections.get(connectionName);
			}
		
		if(ConfigurationHelper.configurationFileExists(connectionName)==false)
		{
			return null;
		}
			
		Connection connection = new Connection(connectionName,input);
		connections.put(connectionName,connection);
		return connection;
	}

	/**
	 * This method loads all the connections.
	 * @return
	 */
	public static ArrayList<Connection> loadConnections() {
		String[] connectionNames=ConfigurationHelper.loadConnections();
		ArrayList<Connection> connections= new ArrayList<Connection>();
		
		for (String connectionName : connectionNames) {
			connections.add(getConnection(connectionName));
		}
		return connections;
	}
	
	/**
	 * This method loads all the connections.
	 * @return
	 */
	public static ArrayList<Connection> loadConnections(ArrayList<String> connectionsList) {
		ArrayList<Connection> connections= new ArrayList<Connection>();
		for (String connectionName : connectionsList) {
			connections.add(getConnection(connectionName));
		}
		return connections;
	}

	/**
	 * Method used to list the current status of the Connection
	 * @param connectionName
	 * @return
	 */
	public static ConnectionStatus getConnectionStatus(String connectionName) {
		Connection connection=getConnection(connectionName);
		if(connection==null)
		{
			return ConnectionStatus.CONNECTION_NOT_FOUND;
		}
		return connection.getStatus();
	}

	/**
	 * This method list active Connections
	 * @return
	 */
	public static ArrayList<Connection> listActiveConnections() {
		ArrayList<Connection> activeConnections= new ArrayList<Connection>();
		
		Set<String> connectionNames = connections.keySet();
	
		
		for (String connectionName : connectionNames) {
			
			Connection connection=connections.get(connectionName);
			if(isActiveConnection(connection))
			{
				activeConnections.add(connection);
			}
		}
		
		return activeConnections;
	}

	private static boolean isActiveConnection(Connection connection) {
		return connection.getStatus()==ConnectionStatus.CONNECTION_ACTIVATED;
	}

	/**
	 * This method stops a connection
	 * @param connectionName
	 */
	public static boolean stopConnection(String connectionName) {
		Connection connection=getConnection(connectionName);
		connection.deactivate();
		return connection.getStatus()==ConnectionStatus.CONNECTION_DEACTIVATED;
	}

	/**
	 * 
	 */
	public static void stopAllConnections() {
		
		ArrayList<Connection> connections=listActiveConnections();
		for (Iterator<Connection> iterator = connections.iterator(); iterator.hasNext();) {
			Connection connection = iterator.next();
			connection.deactivate();
			
		}
		
	}

	/**
	 * This method sets the connection status for a Connection
	 * @param connectionName
	 * @param connectorActivationFailed
	 */
	public static void setConnectionStatus(String connectionName,
			ConnectionStatus status) {
		Connection connection=getConnection(connectionName);
		if(connection==null)
		{
		   // connection not found.
			return ;
		}
		//Already failed need to reset a connection to restart it
		//TODO method and test to be added for rest along with Connection Monitor
		if(ConnectionStatus.hasFailed(connection.getStatus()))
		{
			return;
		}
		connection.setStatus(status);
	}

	/**
	 * Method to start a connection
	 * @param connectionName
	 * @return
	 */
	public static boolean startConnection(String connectionName) {
		Connection connection=getConnection(connectionName);
		if(connection.getStatus().equals(ConnectionStatus.CONNECTION_DEACTIVATED))
			connection.setStatus(ConnectionStatus.CONNECTION_NOT_ACTIVATED);
		connection.activate();
		return connection.getStatus()==ConnectionStatus.CONNECTION_ACTIVATED;
		
	}

	/**
	 * Method returns true if there are active connections
	 * @return
	 */
	public boolean hasActiveConnections() {
		return listActiveConnections().size()>0;
	}

	/**
	 * Method to remove a specific connection
	 * @param connectionName
	 */
	public void removeConnection(String connectionName) {
		connections.remove(connectionName);
		
	}

}