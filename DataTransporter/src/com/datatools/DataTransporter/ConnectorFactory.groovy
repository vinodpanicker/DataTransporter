package com.datatools.DataTransporter

public class ConnectorFactory {
	
	/**
	 * This function loads a Connector for a connectorString ,data and connectionContext
	 * @param connectorString
	 * @param data
	 * @param connectionContext
	 * @return
	 */
	public static Connector getConnector(String connectorString, Data data, ConnectionContext connectionContext) {
		
		ConnectorCfg connectorCfg = ConfigurationHelper.getConnectorCfgFromConnectorString(connectorString)
		def connectorName=connectorCfg.name
		def connectorClassName = DataTransporterConstants.CONNECTOR_NAMESPACE+"."+connectorCfg.type
		Connector connector = (Connector)Class.forName(connectorClassName).newInstance(connectorName);
		//mode should come before name to satisfy condition in Connector setName else mode is always null
		connector.mode =connectorCfg.mode
		connector.name =connectorName
		connector.parameters = connectorCfg.parameters
		connector.fields=getConnectorFields(connectorName)
		connector.connectorFieldMappings=getConnectorFieldMappings(connector.mode,connectionContext.getFieldMapping())
		connector.data = data
		return connector
	}
	
	
	
	
	
	private static String[] getConnectorFieldFromFieldMapping(String mode, String[] fieldMapping) {
		return FieldMappingHelper.getConnectorFields(mode,fieldMapping);
		
	}
	
	private static String[] getConnectorFieldFromFieldMapping(def mode, ArrayList fieldMappingCfgs) {
		return FieldMappingHelper.getConnectorFields(mode,fieldMappingCfgs);
		
	}
	
	private static getConnectorFieldMappings(String mode, String[] fieldMapping) {
		return FieldMappingHelper.getConnectorFieldMappings(mode,fieldMapping);
		
	}
	
	private static getConnectorFieldMappings(def mode, ArrayList fieldMappingCfgs) {
		return FieldMappingHelper.getConnectorFieldMappings(mode,fieldMappingCfgs);
		
	}
	
	
	/**
	 * Fetch a connector in a connection using ConnectionName and ConnectorName
	 * @param connectionName
	 * @param ConnectorName
	 * @return
	 */
	public static Connector getConnector(String connectionName, String connectorName) {
		File connectionCfgFile=new File(DataTransporterConstants.CONFIGFILE_LOCATION+connectionName+".con")
		if(connectionCfgFile.exists())
			return	getConnectorForExistingFile(connectionCfgFile,connectorName,connectionName)
		// to be replaced with NullConnector
		return null
	}
	
	private static Connector getConnectorForExistingFile(File connectionCfgFile, String connectorName) {
		
		
		def connectorCfgs=ConfigurationHelper.getConnectorsCfg(connectionCfgFile)
		def fieldMappingCfgs=ConfigurationHelper.getFieldMappingCfg(connectionCfgFile)
		
		Connector connector=null
		connectorCfgs.each{
			if(it.name==connectorName) {
				def connectorClassName = DataTransporterConstants.CONNECTOR_NAMESPACE+"."+it.type
				connector = (Connector)Class.forName(connectorClassName).newInstance(connectorName);
				connector.type=it.type
				connector.mode=it.mode
				connector.parameters=it.parameters
				connector.fields=getConnectorFields(connectorName)
				connector.connectorFieldMappings=getConnectorFieldMappings(it.mode,fieldMappingCfgs)
				
			}
		}
		
		return connector
	}
	
	//Added overloaded method for setting the connection name inside the connector. 
	//This is a temporary solution, later we need to look at the confiurationHelper for permanent solution similar to hbase testcase 
	private static Connector getConnectorForExistingFile(File connectionCfgFile, String connectorName,String connectionName) {
		
		
		def connectorCfgs=ConfigurationHelper.getConnectorsCfg(connectionCfgFile)
		def fieldMappingCfgs=ConfigurationHelper.getFieldMappingCfg(connectionCfgFile)
		
		Connector connector=null
		connectorCfgs.each{
			if(it.name==connectorName) {
				def connectorClassName = DataTransporterConstants.CONNECTOR_NAMESPACE+"."+it.type
				connector = (Connector)Class.forName(connectorClassName).newInstance(connectorName);
				connector.type=it.type
				connector.mode=it.mode
				connector.parameters=it.parameters
				connector.fields=getConnectorFields(connectorName)
				connector.connectionName=connectionName
				connector.connectorFieldMappings=getConnectorFieldMappings(it.mode,fieldMappingCfgs)

			}
		}
		
		return connector
	}
	
	public static HashMap getConnectorFields(String connectorFileName) {
		
		File connectorCfgFile=new File(DataTransporterConstants.CONFIGFILE_LOCATION+connectorFileName+".con")
		if(connectorCfgFile.exists())
			return	getConnectorFieldsForExistingFile(connectorCfgFile)
		return null
	}
	
	public static HashMap getConnectorFieldsForExistingFile(File connectorCfgFile) {
		HashMap fields=new HashMap()
		connectorCfgFile.eachLine{ line->
			String[] fieldData=line.split(':')
			ConnectorFieldCfg connectorFieldCfg=fieldData
			
			fields.put( connectorFieldCfg.name, connectorFieldCfg)
		}
		
		return fields
		
	}
	
	public static String getConnectorName(String connectionName,String mode) {
		File connectionCfgFile=new File(DataTransporterConstants.CONFIGFILE_LOCATION+connectionName+".con")
		def connectorCfgs=ConfigurationHelper.getConnectorsCfg(connectionCfgFile)
		String connectorName=""
		connectorCfgs.each{
			if(it.mode==mode) {
				connectorName=it.name
			}
		}
		return connectorName
	}
	
}