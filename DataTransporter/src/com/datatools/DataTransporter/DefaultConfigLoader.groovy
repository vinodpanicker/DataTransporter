package com.datatools.DataTransporter



public class DefaultConfigLoader implements ConfigLoader {
	
	
	/**
	 * Loads the schedule configuration for a Connection even before the rest of the connection configuration is loaded
	 */
	public void loadScheduleConfiguration(Connection connection){
		String connectionName=connection.getName()
		ArrayList schedules=new ArrayList<Schedule>()
		File configfile = new File(DataTransporterConstants.CONFIGFILE_LOCATION + connectionName +".con");
		schedules=ScheduleHelper.getSchedulesfromScheduleCfg(ConfigurationHelper.getScheduleCfg(configfile))
		connection.setSchedules(schedules);
	}
	
	/**
	 * This function set the context of a Connection from the connection configuration file
	 * 
	 * This function can also be used to directly set the schedule/connector and the fieldmapping in the connection
	 */
	void loadConnectionConfiguration(Connection connection) {
		
		String connectionName=connection.getName()
		File configfile = new File(DataTransporterConstants.CONFIGFILE_LOCATION + connectionName +".con");
		String[] connectorConfiguration = getConfiguration(configfile,DataTransporterConstants.CONNECTOR_PREFIX)
		String[] fieldMapping = getConfiguration(configfile,DataTransporterConstants.FIELDMAPPING_PREFIX)
		
		ConnectionContext context=Connection.getContext();
		context.setConnectorConfiguration(connectorConfiguration);
		context.setFieldMapping(fieldMapping);
		connection.setContext(context);
		
		
	}
	
	/**
	 * 
	 * @param configFile
	 * @param prefix
	 * @return
	 */
	def String[] getConfiguration(File configFile,String prefix) {
		def result=[] 
		
		configFile.eachLine { 
			
			if(it.contains(prefix)) {
				result.add(it.substring(prefix.length()))
				
			}
		}  
		if(result.size()==0)
			DataTransporter.log.error("Prefix "+prefix+" not found in file "+configFile.getName())
		return result
	}
	
	
	
	@Override
	public void loadConnectorConfiguration(Connector connector) {
		String connectorName=connector.getName()
		connector.fields=ConnectorFactory.getConnectorFields(connectorName)
		
	}
	
	
	
}