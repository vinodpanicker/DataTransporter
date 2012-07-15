package com.datatools.DataTransporter

/****
 * Sample configuration for DebugConnector with filters and transformation
 * connector=name:TestDebugPullConnector,type:DebugConnector,mode:PULL
 * connector=name:TTestDebugPushConnector,type:DebugConnector,mode:PUSH
 * fieldMapping=name:EMPLOYEENO,source:EMPNO,target:EMPNO,filters:~=/E,transformation:toNumerics()
 * fieldMapping=name:EMPLOYEENAME,source:EMPNAME,target:EMP_NAME,filters:~=/V,transformation:toCaps()
 ***/

public class TestConfigurationLoader extends DefaultConfigLoader {
	
	public void loadConnectionConfiguration(Connection connection) {
		File configFile = new File(DataTransporterConstants.CONFIGFILE_LOCATION + connection.name +".con");
		//String[] connectorConfiguration = getConnectorConfiguration(configFile);
		//String[] fieldMapping = getFieldMapping(configFile);
		String[] connectorConfiguration = getConfiguration(configFile,DataTransporterConstants.CONNECTOR_PREFIX)
		String[] fieldMapping = getConfiguration(configFile,DataTransporterConstants.FIELDMAPPING_PREFIX)
		
		ConnectionContext context=Connection.getContext();
		context.setConnectorConfiguration(connectorConfiguration);
		context.setFieldMapping(fieldMapping);
		
		connection.setContext(context);
		connection.setStatus(Connection.CONF_LOADED);
		
		int i = 0;
		def connectorsList = [] 
		context.getConnectorConfiguration().each {
			Connector connector = ConnectorFactory.getConnector(context.getConnectorConfiguration()[i++],connection.getConnectionData(),context);
			connectorsList.add(connector);
		}
		connection.setConnectors(connectorsList)
		
	}
	
	
	
	
	def String[] getConnectorConfiguration(File configFile) {
		def result=[] 
		
		configFile.eachLine { 
			
			if(it.contains("connector=")) {
				result.add(it.substring("connector=".length()))
				
			}
		}  
		
		return result
	}
	
	def String[] getFieldMapping(File configFile) {
		def result=[] 
		
		configFile.eachLine { 
			
			if(it.contains("fieldMapping=")) {
				result.add(it.substring("fieldMapping=".length()))
				
			}
		}  
		
		return result
	}
	
	
	public void loadConnectionConfiguration1(String name, Connection connection) {
		
		
		
		
		//		DebugConnector debugConnectorPull = new DebugConnector();
		//		DebugPullConnector debugConnectorPull = new DebugPullConnector();
		//		debugConnectorPull.setName("TestDebugPullConnector");
		//		debugConnectorPull.setMode(Connector.MODE_PULL);
		//		debugConnectorPull.setData(connection.getConnectionTaskQueue());
		//		
		////		DebugConnector debugConnectorPush = new DebugConnector();
		//		DebugPushConnector debugConnectorPush = new DebugPushConnector();
		//		debugConnectorPush.setName("TestDebugPushConnector");
		//		debugConnectorPush.setMode(Connector.MODE_PUSH);
		//		debugConnectorPush.setData(connection.getConnectionTaskQueue());
		//		
		//		connectors.add(debugConnectorPull);
		//		connectors.add(debugConnectorPush);
		//
		//		connection.setConnectors(connectors);
		//
		//		FieldMapping fm1 = new FieldMapping();
		//		fm1.setFieldMappingName("EMPLOYEENO");
		//		fm1.setSourceField("EMPNO");
		//		fm1.setTargetField("EMP_NO");
		//
		//		FieldMapping fm2 = new FieldMapping();
		//		fm2.setFieldMappingName("EMPLOYEENAME");
		//		fm2.setSourceField("EMPNAME");
		//		fm2.setTargetField("EMP_NAME");
		//
		//		ArrayList fieldMappings = new ArrayList();
		//
		//		fieldMappings.add(fm1);
		//		fieldMappings.add(fm2);
		//		connection.getFieldMappings().add(fm1);
		//		connection.getFieldMappings().add(fm2);
		//
		//		// connection.setFieldMappings(fieldMappings);
		
	}
	
	public static Connection getConnection(String connectionName) {
		
		Connection connection = new Connection();
		loadConnectionConfiguration(connectionName,connection)
		return connection;
	}
	
	
}