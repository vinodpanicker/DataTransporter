package com.datatools.DataTransporter

/**
 * 
 * @author Vinod Panicker
 *
 */
public class ConfigurationHelper {
	


	/**
	 * This method is should be called only from test class 
	 * @param connectionName
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 * @return
	 */
	public static String getConnectionConfigurationAsString(String connectionName,
	ArrayList<ConnectorCfg> connectorCfgs,
	ArrayList<FieldMappingCfg> fieldMappingCfgs) {
		return ConfigurationBuilder.buildConnectionCfg(connectorCfgs,
		fieldMappingCfgs,null)
	}
	
	/**
	 * This method is should be called only from test class
	 * @param connectionName
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 * @return
	 */
	public static String getConnectionConfigurationAsString(String string,
	ArrayList<ConnectorCfg> connectorCfgs,
	ArrayList<FieldMappingCfg> fieldMappingCfgs,
	ArrayList<ScheduleCfg> scheduleCfgs) {
		return ConfigurationBuilder.buildConnectionCfg(connectorCfgs,
		fieldMappingCfgs,scheduleCfgs)
	}
	
	/**
	 *  This method loads connection files in the config folder
	 * 
	 * ps: All connection files must end with XYZConnection.con format.
	 * 
	 * @return
	 */
	public static String[] loadConnections() {

		def connectionNames = []
		try{
		connectionNames = getConnectionNames()
		}
		catch(FileNotFoundException e){
			DataTransporter.log.error ("File "+e.getMessage()+" not found")
		}
		return connectionNames;
	}
	
	
	/**
	 * This method returns a FieldMappingCfg for a connection
	 * @param fieldMappingString
	 * @return
	 */
	public static FieldMappingCfg getConnectionFieldMappingCfg(fieldMappingString) {
		GroovyShell gs= new GroovyShell(); 
		String expression="["+fieldMappingString+"]"
		def result=gs.evaluate(expression)
		
		def fieldMappingName=result.get("name")
		def fieldMappingSource=result.get("source")
		def fieldMappingTarget=result.get("target")
		//For Filter as well as Transformation multiple filter or transformation will be return
		// the order of the filter and transformation has to be determined and stated.
		def fieldMappingFilters=[]
		fieldMappingFilters <<result.get("filter")
		def fieldMappingTransformations=[]
		fieldMappingTransformations<<result.get("transformation")
		FieldMappingCfg fieldMappingCfg=new FieldMappingCfg(source:fieldMappingSource,target:fieldMappingTarget,filters:fieldMappingFilters,transformations:fieldMappingTransformations,name:fieldMappingName)
		return fieldMappingCfg
	}
	/**
	 * Method returns a ConnectorCfg for a connection
	 * @param connectorString
	 * @return
	 */
	private static ConnectorCfg getConnectorCfgFromConnectorString(connectorString) {
		GroovyShell gs= new GroovyShell(); 
		String expression="["+connectorString+"]"
		def result=gs.evaluate(expression)
		def connectorName
		def connectorType
		def connectorMode
		if(result.get("name")!=null)
			connectorName=result.get("name")
		else
			DataTransporter.log.error("Connector name is not specified!!!");
		if(result.get("type")!=null)
			connectorType=result.get("type")
		else
			DataTransporter.log.error("Connector type is not specified!!!");
		if(result.get("mode")!=null)
			connectorMode=result.get("mode")
		else
			DataTransporter.log.error("Connector mode is not specified!!!");
		result.keySet().removeAll( ['name','mode','type'] as Set)
		def connectorParameters=result
		ConnectorCfg connectorCfg=new ConnectorCfg(name:connectorName,type:connectorType,mode:connectorMode,parameters:connectorParameters)
		return connectorCfg
	}
	/**
	 * Method returns a ScheduleCfg for a connection
	 * @param scheduleString
	 * @return
	 */
	private static ScheduleCfg  getScheduleCfgFromScheduleString(scheduleString)
	{
		GroovyShell gs= new GroovyShell(); 
		String expression="["+scheduleString+"]"
		def result=gs.evaluate(expression)
		def workDays=result.get("workdays")
		def officeHours=result.get("officehours")
		def startEveryXMinutes=result.get("starteveryxminutes")
		def startEveryXHours=result.get("starteveryxhours")
	
		ScheduleCfg scheduleCfg=new ScheduleCfg(workDays:workDays,officeHours:officeHours,startEveryXMinutes:startEveryXMinutes,startEveryXHours:startEveryXHours)
		return scheduleCfg
	}
	/**
	 * Method used to by configuration generation tools to create config files
	 * @param connectionName
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 * @param scheduleCfgs
	 * @return
	 */
	public static boolean createConnectionCfgFile(String connectionName,
	ArrayList<ConnectorCfg> connectorCfgs,
	ArrayList<FieldMappingCfg> fieldMappingCfgs, ArrayList<ScheduleCfg> scheduleCfgs) {
		return ConfigurationBuilder.buildConnectionCfg(connectionName,
		connectorCfgs, fieldMappingCfgs,scheduleCfgs);
	}	
	
	/**
	 * Method used to by configuration generation tools to create config files
	 * @param connectionName
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 * @return
	 */
	public static boolean createConnectionCfgFile(String connectionName,
	ArrayList<ConnectorCfg> connectorCfgs,
	ArrayList<FieldMappingCfg> fieldMappingCfgs) {
		return ConfigurationBuilder.buildConnectionCfg(connectionName,
		connectorCfgs, fieldMappingCfgs,null);
	}
	
	/**
	 * This method would overwrite any configuration files, to be used only to regenerate Connector Config files from a connection
	 * @param connectionName
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 */
	public static boolean createConnectorCfgFilesFromConnection(
	String connectionName, ArrayList<ConnectorCfg> connectorCfgs,
	ArrayList<FieldMappingCfg> fieldMappingCfgs) {
		return ConfigurationBuilder.buildConnectorCfgFromConnection(connectionName,
		connectorCfgs, fieldMappingCfgs);
		
	}
	
	/***
	 * Method used by configuration tools to generate Connector config files
	 * @param connectorName
	 * @param connectorFieldCfgs
	 * @return
	 */
	public static boolean createConnectorCfgFileFromFieldCfgs(
	String connectorName,
	ArrayList<ConnectorFieldCfg> connectorFieldCfgs) {
		
		return  ConfigurationBuilder.buildConnectorCfgFromFieldCfgs(connectorName,connectorFieldCfgs);
	}
	
	/**
	 * Gets the FieldMappingCfg from a Connection Configuration File
	 * @param connectionCfgFile
	 * @return
	 */
	public static ArrayList getFieldMappingCfg(File connectionCfgFile) {
		def fieldMappingCfgs=[]
		
		connectionCfgFile.eachLine{ line->
			
			if(line.find(DataTransporterConstants.FIELDMAPPING_PREFIX)) {
				// only if a fieldMapping configuration line
				def fieldMappingString=line.replace(DataTransporterConstants.FIELDMAPPING_PREFIX,'')
				FieldMappingCfg fieldMappingCfg = getConnectionFieldMappingCfg(fieldMappingString)
				fieldMappingCfgs.add(fieldMappingCfg)
			}
		}  
		
		return fieldMappingCfgs
		
	}
	
	/**
	 * Gets the ConnectionCfg from a Connection Configuration File
	 * 
	 * @param connectionCfgFile
	 * @return
	 */
	public static ArrayList getConnectorsCfg(File connectionCfgFile) {
		def connectorCfgs=[]
		connectionCfgFile.eachLine{ line->
			
			if(line.find(DataTransporterConstants.CONNECTOR_PREFIX)) {
				// only if a connector configuration line
				def connectorString=line.replace(DataTransporterConstants.CONNECTOR_PREFIX,'')
//				println connectorString
				DataTransporter.log.debug(connectorString)
				ConnectorCfg connectorCfg = getConnectorCfgFromConnectorString(connectorString)
				connectorCfgs.add(connectorCfg)
			}
		}  
		return connectorCfgs
	}
	
	/**
	 * Gets the ScheduleCfg from a Connection Configuration File
	 * 
	 * @param connectionCfgFile
	 * @return
	 */
	public static ArrayList getScheduleCfg(File connectionCfgFile) {
		def scheduleCfgs=[]
		connectionCfgFile.eachLine{ line->
			
			if(line.find(DataTransporterConstants.SCHEDULE_PREFIX)) {
				// only if a connector configuration line
				def scheduleString=line.replace(DataTransporterConstants.SCHEDULE_PREFIX,'')
//				println scheduleString
				DataTransporter.log.debug (scheduleString)
				ScheduleCfg scheduleCfg = ConfigurationHelper.getScheduleCfgFromScheduleString(scheduleString)
				scheduleCfgs.add(scheduleCfg)
			}
		}  
		return scheduleCfgs
	}
	
	/**
	 * This method is used to check if Connection Configuration file exists in the .config folder
	 * @param connectionName
	 * @return
	 */
	public static boolean configurationFileExists(String connectionName) {
		def connectionNames = []
		    connectionNames = getConnectionNames()
		
		return connectionNames.contains(connectionName);
	}
	
	private static getConnectionNames() throws FileNotFoundException{
		def pathname=DataTransporterConstants.CONFIGFILE_LOCATION
		
		def connectionNames=[]
		new File(pathname).eachFile{
			
			def filename=it.name
			if(filename.endsWith(DataTransporterConstants.CONNECTION_FILE_EXTENSION)) {
				connectionNames<<filename.replace(DataTransporterConstants.CONNECTION_FILE_EXTENSION,"Connection")
			}
		}
		return connectionNames
	}
}