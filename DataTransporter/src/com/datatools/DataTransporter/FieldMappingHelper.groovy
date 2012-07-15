package com.datatools.DataTransporter

import java.util.ArrayList;
import java.util.List;
/**
 *  FieldMappingHelper will help in getting the source//target/transformations and filters required
 *     
 * 
 * @author Vinod Panicker
 *
 */
public class FieldMappingHelper {
	
	public static String[] getConnectorFields(String mode,
	ArrayList<FieldMappingCfg> fieldMappingCfgs) {
		
		def fieldsFromFieldMapping=[]
		
		if(mode==Connector.MODE_PULL)
		{	
			fieldsFromFieldMapping = getFieldToPullFrom(fieldMappingCfgs, fieldsFromFieldMapping)
		}
		else
		{
			fieldsFromFieldMapping = getFieldsToPushFrom(fieldMappingCfgs, fieldsFromFieldMapping)
		}
		
		return fieldsFromFieldMapping;
	}
	
	private static getFieldsToPushFrom(ArrayList<FieldMappingCfg> fieldMappingCfgs, fieldsFromFieldMapping) {
		fieldMappingCfgs.each { 
			fieldsFromFieldMapping<<it.target
		}	
		return fieldsFromFieldMapping
	}
	
	private static getFieldToPullFrom(ArrayList<FieldMappingCfg> fieldMappingCfgs, fieldsFromFieldMapping) {
		fieldMappingCfgs.each { 
			fieldsFromFieldMapping<<it.source
			
		}
		return fieldsFromFieldMapping
	}
	
	/**
	 * This method returns the fields of the connector present in the fieldmapping
	 * @param mode
	 * @param fieldMappingfromConnectionContext
	 * @return
	 */
	public static String[] getConnectorFields(String mode,
	String[] fieldMappingfromConnectionContext) {
		
		def connectorFields=[]
		fieldMappingfromConnectionContext.each{
			FieldMapping fieldMapping=new FieldMapping();
			fieldMapping.setFieldMapping(it)
			if(mode==Connector.MODE_PULL)
			{
				connectorFields << fieldMapping.sourceField
			}
			else
			{
				connectorFields << fieldMapping.targetField
			}
		}
		return connectorFields;
	}
	/**
	 * This method returns a list of ConnecterFieldMapping for a connections fieldmappings.
	 * @param mode
	 * @param fieldMappingCfgs
	 * @return
	 */
	public static List getConnectorFieldMappings(String mode,
	ArrayList<FieldMappingCfg> fieldMappingCfgs) {
		def fieldmappings=[]
		if(mode==Connector.MODE_PULL){                  
			fieldmappings = getConnectionFieldMappingForPULLConnector(fieldMappingCfgs, fieldmappings)
		}
		else{
			fieldmappings = getConnectionFieldMappingForPUSHConnector(fieldMappingCfgs, fieldmappings)
		}
		return fieldmappings;
	}	
	
	
	private static getConnectionFieldMappingForPULLConnector(ArrayList<FieldMappingCfg> fieldMappingCfgs, fieldmappings) {
		fieldMappingCfgs.each{
			
			ConnectorFieldMapping connectorFieldMapping= new ConnectorFieldMapping()
			connectorFieldMapping.name=it.source
			connectorFieldMapping.filters=it.filters
			connectorFieldMapping.transformations=it.transformations
			connectorFieldMapping.connectionFieldMappingName=it.name
			fieldmappings << connectorFieldMapping
		}
		return fieldmappings
	}
	
	private static getConnectionFieldMappingForPUSHConnector(ArrayList<FieldMappingCfg> fieldMappingCfgs, fieldmappings) {
		fieldMappingCfgs.each{
			
			ConnectorFieldMapping connectorFieldMapping= new ConnectorFieldMapping()
			connectorFieldMapping.name=it.target
			connectorFieldMapping.filters=it.filters
			connectorFieldMapping.transformations=it.transformations
			connectorFieldMapping.connectionFieldMappingName=it.name
			fieldmappings << connectorFieldMapping
		}
		return fieldmappings
	}	
	

	private static ArrayList getConnectorFieldMappingsList(String mode,
			String[] fieldMappingfromConnectionContext){
		
			def fieldMappingCfgs=[]
            fieldMappingfromConnectionContext.each{ fieldMappingString ->

			FieldMappingCfg fieldMappingCfg = ConfigurationHelper.getConnectionFieldMappingCfg(fieldMappingString)
			fieldMappingCfgs.add(fieldMappingCfg)
		}
	return fieldMappingCfgs
  	}
		
	public static ArrayList getConnectorFieldMappings(String mode,
			String[] fieldMappingfromConnectionContext) {
		return getConnectorFieldMappings(mode,getConnectorFieldMappingsList(mode,fieldMappingfromConnectionContext))
	}

	/**
	 * Method to be used in the Connectors to get ConnectorFields for the connector
	 * @param connectorFieldMappings
	 * @return
	 */
	public static String[] getConnectorFields(List<ConnectorFieldMapping> connectorFieldMappings) {
		def connectorFields=[]
		connectorFieldMappings.each{
		
				connectorFields<<it.name
			}
		return connectorFields
	}	
	
}