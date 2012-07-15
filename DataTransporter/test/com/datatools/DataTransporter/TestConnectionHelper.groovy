package com.datatools.DataTransporter


public class TestConnectionHelper {
	static Logger log=new Logger().getLogger(this.getClass());
	
	/**
	 * Method to create a connectorCfg for connectors used for testing connections
	 * @param mode
	 * @param connectorType
	 * @deprecated
	 * @param connectorName
	 * @return
	 */
	public static ConnectorCfg setupDefaultConnectorCfg(String mode,
	String connectorType,String connectorName) {
		HashMap<String, String> additionalParameters = new HashMap<String, String>();
		additionalParameters.put("tablename", "input");
		additionalParameters.put("families", "SourceField3");
		additionalParameters.put("qualifiers", "SourceField3:qualifier2");
		additionalParameters.put("qualifiers", "SourceField3:qualifier1");
		additionalParameters.put("resources", "111");
		
		ConnectorCfg connectorCfg = setupDefaultConnectorCfgWithParams(connectorType, mode, connectorName, additionalParameters);
		return connectorCfg;
		
	}
	
	public static ConnectorCfg setupDefaultConnectorCfgWithParams(String connectorType,String mode,  String connectorName, HashMap<String,String> additionalParameters) {
		ConnectorCfg connectorCfg = new ConnectorCfg(connectorType,mode, connectorName, additionalParameters)
		return connectorCfg
	}
	/**
	 * Method creates as many fieldmappings as  specified by numberOfFields
	 * Used for testing connections
	 * @param numberOfFields
	 * @return
	 */
	public static ArrayList<FieldMappingCfg> setupDefaultFieldMappingCfgs(int numberOfFields) {
		ArrayList<FieldMappingCfg> fieldMappingCfgs = new ArrayList<FieldMappingCfg>();
		FieldMappingCfg fieldMappingCfg=null;
		
		(1..numberOfFields).each{
			
			//fieldMappingCfg = new FieldMappingCfg("SourceField$it","TargetField$it", [" Filter$it"],["TransformationMethod$it"], "FieldMappingName$it")
			fieldMappingCfg = new FieldMappingCfg("SourceField$it","TargetField$it", [""],[""], "FieldMappingName$it")
			fieldMappingCfgs.add(fieldMappingCfg);
		}
		return fieldMappingCfgs;
	}
	
	/**
	 * Default setup for Default FieldMapping creates 10 fields, used to test creation of field mapping. 
	 * @return
	 */
	public static ArrayList<FieldMappingCfg> setupDefaultFieldMappingCfgs(){
		def data=[]
		(1..10){
			data[it][1]="SourceField$it"
			data[it][2]="TargetField$it"
			data[it][3]="Filter$it"
			data[it][4]="TransformationMethod$it"
			data[it][5]="FieldMappingName$it"
		}
		return setupDefaultFieldMappingCfgs(data)
	}
	
	public static ArrayList<FieldMappingCfg> setupDefaultFieldMappingCfgs(def data){
		ArrayList<FieldMappingCfg> fieldMappingCfgs = new ArrayList<FieldMappingCfg>();
		
		
		data.each{
			
			FieldMappingCfg fieldMappingCfg = new FieldMappingCfg("$it[1]","$it[2]", ["$it[3]"],["$it[4]"], "$it[5]")
			fieldMappingCfgs.add(fieldMappingCfg);
		}
		return fieldMappingCfgs;
		
	}
	
	public static ArrayList<ConnectorFieldCfg> setupDefaultConnectorFieldCfg(
	int numberOfFields,String fieldPrefix) {
		ArrayList<ConnectorFieldCfg> connectorFieldCfgs = new ArrayList<ConnectorFieldCfg>();
		(1..numberOfFields).each{
			
			ConnectorFieldCfg connectorFieldCfg = new ConnectorFieldCfg(fieldPrefix+"$it", "String", "10", "$it");
			
			connectorFieldCfgs.add(connectorFieldCfg);
		}
		return connectorFieldCfgs
	}	
	
	public static ArrayList<ScheduleCfg> setupDefaultScheduleCfgs() {
		String workDays = "Calendar.MONDAY..Calendar.FRIDAY";
		String officeHours = "8..18";
		String startEveryXMinutes = "1";
		String startEveryXHours = "1";
		
		ArrayList<ScheduleCfg> scheduleCfgs = new ArrayList<ScheduleCfg>();
		ScheduleCfg scheduleCfg = new ScheduleCfg(
				workDays, officeHours, startEveryXMinutes, startEveryXHours);
		scheduleCfgs.add(scheduleCfg);
		
		return scheduleCfgs;
		
	}	
	
	public static void printContent(ArrayList recordsToWrite,String message) {
		log.info message
		log.info recordsToWrite.inspect();
		recordsToWrite.each{
			it.each{ entry-> 
				print entry.key +":"
				print entry.value.inspect() +","
			}
			println ""
		}
		
	}
	
	/**
	 * Create fieldNames from and array of fieldNames
	 * @param fieldNames
	 * @return
	 */
	public static ArrayList<ConnectorFieldCfg> setupDefaultConnectorFieldCfg(
	String[] fieldNames) {
		ArrayList<ConnectorFieldCfg> connectorFieldCfgs = new ArrayList<ConnectorFieldCfg>();
		fieldNames.each{
			
			ConnectorFieldCfg connectorFieldCfg = new ConnectorFieldCfg("$it", "String", "10", "$it");
			
			connectorFieldCfgs.add(connectorFieldCfg);
		}
		return connectorFieldCfgs
	}	
	
	
	/**
	 * Create Default Field cfg is the fieldnames and datatypes are given
	 * @param fieldNames
	 * @param datatypes
	 * @return
	 */
	public static ArrayList<ConnectorFieldCfg> setupDefaultConnectorFieldCfg(
	String[] fieldNames,String[] datatypes) {
		ArrayList<ConnectorFieldCfg> connectorFieldCfgs = new ArrayList<ConnectorFieldCfg>();
		int i=0;
		fieldNames.each{
			
			ConnectorFieldCfg connectorFieldCfg = new ConnectorFieldCfg("$it", datatypes==null||datatypes[i]==null?"String":datatypes[i], "10", "$it");
			i++
			connectorFieldCfgs.add(connectorFieldCfg);
		}
		return connectorFieldCfgs
	}	
	
	public static ArrayList<FieldMappingCfg> setupFieldMappingCfgs(String[] sourceFieldNames, String[] targetFieldNames,
	String[] filters, String[] transformations) {
		ArrayList<FieldMappingCfg> fieldMappingCfgs = new ArrayList<FieldMappingCfg>();
		FieldMappingCfg fieldMappingCfg=null;
		int numberOfFields=sourceFieldNames.length
		int i=0
		(1..numberOfFields).each{
			
			def sourceField=(sourceFieldNames==null||sourceFieldNames[i]==null)?("sourceField"+i):sourceFieldNames[i]
			def targetField=(targetFieldNames==null||targetFieldNames[i]==null)?("TargetField"+i):targetFieldNames[i]
			def filterNames=(filters==null||filters[i]==null)?"":filters[i]
			def transformationNames=(transformations==null||transformations[i]==null)?"":transformations[i]
			def fieldMappingName="FieldMappingName"+i
			
			fieldMappingCfg = new FieldMappingCfg(sourceField,targetField, filterNames,transformationNames, fieldMappingName)
			fieldMappingCfgs.add(fieldMappingCfg)
			i++
		}
		return fieldMappingCfgs;

	}
}
