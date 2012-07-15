package com.datatools.DataTransporter;

import java.util.ArrayList;

/**
 * Class has deprecated methods ,class maybe removed later
 * @author Vinod Panicker
 *
 */
public class ConfigurationFactory {

	/**
	 * This method returns a string representation of the ConnectionCfg
	 * @param connectionName
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 * @deprecated use Configuration ConfigurationHelper getConnectionConfigurationAsString
	 * @return
	 */
	public static String createConnectionCfg(String connectionName,
			ArrayList<ConnectorCfg> connectorCfgs,
			ArrayList<FieldMappingCfg> fieldMappingCfgs) {
		return ConfigurationHelper.getConnectionConfigurationAsString(connectionName, connectorCfgs, fieldMappingCfgs);	

	}

	/**
	 * @deprecated use Configuration ConfigurationHelper getConnectionConfigurationAsString
	 * @param string
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 * @param scheduleCfgs
	 * @return
	 */
	public static String createConnectionCfg(String string,
			ArrayList<ConnectorCfg> connectorCfgs,
			ArrayList<FieldMappingCfg> fieldMappingCfgs,
			ArrayList<ScheduleCfg> scheduleCfgs) {
		
		return ConfigurationHelper.getConnectionConfigurationAsString(string, connectorCfgs, fieldMappingCfgs, scheduleCfgs);
	}
	
	/**
	 * @deprecated use Configuration ConfigurationHelper.createConnectionCfgFile
	 * @param connectionName
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 * @param scheduleCfgs
	 * @return
	 */
	public static boolean createConnectionCfgFile(String connectionName,
			ArrayList<ConnectorCfg> connectorCfgs,
			ArrayList<FieldMappingCfg> fieldMappingCfgs, ArrayList<ScheduleCfg> scheduleCfgs) {
//		return ConfigurationBuilder.buildConnectionCfg(connectionName,
//				connectorCfgs, fieldMappingCfgs,null);
		return ConfigurationHelper.createConnectionCfgFile(connectionName,
				connectorCfgs, fieldMappingCfgs,scheduleCfgs);
	}
	/**
	 * @deprecated use Configuration ConfigurationHelper.createConnectionCfgFile
	 * @param connectionName
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 * @return
	 */
	public static boolean createConnectionCfgFile(String connectionName,
			ArrayList<ConnectorCfg> connectorCfgs,
			ArrayList<FieldMappingCfg> fieldMappingCfgs) {
	//	return ConfigurationBuilder.buildConnectionCfg(connectionName,
	//			connectorCfgs, fieldMappingCfgs,null);
		return ConfigurationHelper.createConnectionCfgFile(connectionName,
				connectorCfgs, fieldMappingCfgs,null);
	}
	/***
	 * This method would overwrite any configuration files, to be used only to regenerate Connector Config files from a connection
	 * @deprecated use Configuration ConfigurationHelper.createConnectorCfgFilesFromConnection
	 * @param string
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 * @return
	 */

	public static boolean createConnectorCfgFilesFromConnection(String connectionName,
			ArrayList<ConnectorCfg> connectorCfgs,
			ArrayList<FieldMappingCfg> fieldMappingCfgs) {
		
//		return ConfigurationBuilder.buildConnectorCfgFromConnection(connectionName,
//				connectorCfgs, fieldMappingCfgs);
		return ConfigurationHelper.createConnectorCfgFilesFromConnection(connectionName,
				connectorCfgs, fieldMappingCfgs);
	}

	/**
	 * @deprecated use Configuration ConfigurationHelper.createConnectorCfgFileFromFieldCfgs
	 * @param connectorName
	 * @param connectorFieldCfgs
	 * @return
	 */
	public static boolean createConnectorCfgFileFromFieldCfgs(String connectorName,
			ArrayList<ConnectorFieldCfg> connectorFieldCfgs) {
		
		//return ConfigurationBuilder.buildConnectorCfgFromFieldCfgs(connectorName,connectorFieldCfgs);
		return ConfigurationHelper.createConnectorCfgFileFromFieldCfgs(connectorName,connectorFieldCfgs);
	}


}
