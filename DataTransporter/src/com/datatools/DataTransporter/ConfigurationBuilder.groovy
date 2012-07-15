package com.datatools.DataTransporter

/***
 * Configuration Builder class used to build configuration files for connections and connectors 
 * @author Vinod Panicker
 *
 */

public class ConfigurationBuilder {
	
	static Logger log=new Logger().getLogger(this.getClass());
	
	public static String buildConnectionCfg(
	ArrayList<ConnectorCfg> connectorCfgs,
	ArrayList<FieldMappingCfg> fieldMappingCfgs, ArrayList<ScheduleCfg> scheduleCfgs) {
		
		def result='';
		def connectionBuilder = new ConnectionBuilder()
		if(connectorCfgs==null||connectorCfgs.isEmpty())
			return
//		println connectorCfgs.dump()
		log.debug(connectorCfgs.dump())
		
		connectorCfgs.each{
			String connectorName =it.name
			String connectorType= it.type
			String connectorMode=it.mode
			Map parameters=it.parameters
			
			connectionBuilder.connector() {
				name("${connectorName}")
				type("${connectorType}")
				mode("${connectorMode}")
				parameters.each{ entry ->
					"${entry.key}"("${entry.value}")
				}
			}
			
		}
		if(fieldMappingCfgs==null||fieldMappingCfgs.isEmpty())
			return connectionBuilder.result
		
//		println fieldMappingCfgs.dump()
		log.debug(fieldMappingCfgs.dump())
		
		fieldMappingCfgs.each{
			String fieldMappingName =it.name
			String fieldMappingSource= it.source
			String fieldMappingTarget=it.target
			String[] fieldMappingFilters=it.filters
			String[] fieldMappingTransformations=it.transformations
			
			connectionBuilder.fieldMapping(){
				name("${fieldMappingName}")
				source("${fieldMappingSource}")
				target("${fieldMappingTarget}")
				fieldMappingFilters.each { fieldMappingFilter-> 
					filter("${fieldMappingFilter}")
				}
				fieldMappingTransformations.each{ fieldMappingTransformation->
					
					transformation("${fieldMappingTransformation}")
				}
			}
			
		}
		
//		println connectionBuilder.result
		log.debug(connectionBuilder.result)
		if(scheduleCfgs==null||scheduleCfgs.isEmpty())
			return connectionBuilder.result
			
			scheduleCfgs.each{
			
			def workDays =it.workDays?it.workDays:DataTransporterConstants.SCHEDULER_DEFAULT_FIRST_WORK_DAY..DataTransporterConstants.SCHEDULER_DEFAULT_LAST_WORK_DAY
			def officeHours= it.officeHours?it.officeHours:DataTransporterConstants.SCHEDULER_DEFAULT_START_OFFICE_HOURS..DataTransporterConstants.SCHEDULER_DEFAULT_END_OFFICE_HOURS
			int startEveryXMinutes=it.startEveryXMinutes?it.startEveryXMinutes:DataTransporterConstants.SCHEDULER_DEFAULT_FOR_EVERY_XMINUTES
			int startEveryXHours=it.startEveryXHours?it.startEveryXHours:DataTransporterConstants.SCHEDULER_DEFAULT_FOR_EVERY_XHOURS
				
				connectionBuilder.schedule() {
					workdays("${workDays}")
					officehours("${officeHours}")
					starteveryxminutes("${startEveryXMinutes}")
					starteveryxhours("${startEveryXHours}")
				}
				
			}
		return connectionBuilder.result
	}
	
	
	public static boolean buildConnectionCfg(String connectionName,
	ArrayList<ConnectorCfg> connectorCfgs,
	ArrayList<FieldMappingCfg> fieldMappingCfgs,  ArrayList<ScheduleCfg> scheduleCfgs) {
		
		File connectionCfgFile= new File(DataTransporterConstants.CONFIGFILE_LOCATION+connectionName+".con")
		
		initializeConfigurationFile(connectionCfgFile)
		connectionCfgFile<<buildConnectionCfg(connectorCfgs,fieldMappingCfgs,scheduleCfgs)
		
		return true;
	}	
	
	public static initializeConfigurationFile(File configfile) {
		if(configfile.exists()) {
			configfile.delete()
			configfile.createNewFile()
		}
	}
	
	
	/**
	 * This method would overwrite any configuration files, to be used only to regenerate Connector Config files from a connection
	 * @param connectionName
	 * @param connectorCfgs
	 * @param fieldMappingCfgs
	 * @return
	 */
	
	public static boolean buildConnectorCfgFromConnection(
	String connectionName, ArrayList<ConnectorCfg> connectorCfgs,
	ArrayList<FieldMappingCfg> fieldMappingCfgs) {
		
		if(connectorCfgs==null||connectorCfgs.isEmpty())
			return false
		
//		println connectorCfgs.dump()
		log.debug(connectorCfgs.dump())
		
		File sourceConnectorCfgFile=null;
		File targetConnectorCfgFile=null;
		
		connectorCfgs.each{
			String connectorFileName =it.name
			
			String connectorMode=it.mode
			if(it.mode==Connector.MODE_PULL) {
				sourceConnectorCfgFile=new File(DataTransporterConstants.CONFIGFILE_LOCATION+connectorFileName+".con")
				initializeConfigurationFile(sourceConnectorCfgFile)
			}
			else {
				targetConnectorCfgFile=new File(DataTransporterConstants.CONFIGFILE_LOCATION+connectorFileName+".con")
				initializeConfigurationFile(targetConnectorCfgFile)
			}
			
		}
		
		if(fieldMappingCfgs==null||fieldMappingCfgs.isEmpty())
			return false
		
		fieldMappingCfgs.each{
			sourceConnectorCfgFile << it.source
			sourceConnectorCfgFile << ":\n"
			targetConnectorCfgFile << it.target
			targetConnectorCfgFile << ":\n"
		}
		
		
		return true;
	}
	
	/**
	 * This method creates a configuration file from collection of connectorFieldCfgs
	 * @param connectorName
	 * @param connectorFieldCfgs
	 * @return
	 */
	public static boolean buildConnectorCfgFromFieldCfgs(String connectorFileName,
	ArrayList<ConnectorFieldCfg> connectorFieldCfgs) {
		File connectorCfgFile=new File(DataTransporterConstants.CONFIGFILE_LOCATION+connectorFileName+".con")
		initializeConfigurationFile(connectorCfgFile)
		
		connectorFieldCfgs.each{
			connectorCfgFile << it?.name
			connectorCfgFile<<':'
			connectorCfgFile << it?.dataType
			connectorCfgFile<<':'
			connectorCfgFile << it?.fieldWidth
			connectorCfgFile<<':'
			connectorCfgFile << it?.fieldWidth
			connectorCfgFile<<'\n'
		}
		
		return true;
	}
}