package com.datatools.DataTransporter;

public class ConnectionContext {

	String[] connectorConfiguration=null;
    String[] fieldMapping=null;
    

	public void setConnectorConfiguration(String[] connectorConfiguration) {
		this.connectorConfiguration=connectorConfiguration;
		
	}

	public void setFieldMapping(String[] fieldMapping) {
		this.fieldMapping=fieldMapping;
		
	}

	/**
	 * Context lookup methods for Push connector to write, 
	 * get the source field from field mapping when target field name is available
	 * @param targetFieldName
	 * @return
	 * This has to be cached.
	 */
	public String getSourceField(String targetFieldName) {
		
		
		FieldMapping fieldMap=new FieldMapping();
		for (int i = 0; i < fieldMapping.length; i++) {
			
			String fieldMappingString = fieldMapping[i];
			fieldMap.setFieldMapping(fieldMappingString);
			
			if(fieldMap.hasTargetField(targetFieldName))
			{
				return fieldMap.getSourceField();
			}
		}
		return null;
	}

	/**
	 * Context lookup methods for Push connector to write, 
	 * get the filter expression when target field name is available
	 * @param targetFieldName
	 * @return
	 * This has to be cached.
	 */
	public String[] getSourceFilter(String targetFieldName) {
		FieldMapping fieldMap=new FieldMapping();
		for (int i = 0; i < fieldMapping.length; i++) {
			
			String fieldMappingString = fieldMapping[i];
			fieldMap= new FieldMapping(); 
			fieldMap.setFieldMapping(fieldMappingString);
			
			if(fieldMap.hasTargetField(targetFieldName))
			{
				return fieldMap.getFilters();
			}
		}
		return null;
	}

	/**
	 * Context lookup methods for Push connector to write, 
	 * get the transformation method when target field name is available
	 * @param targetFieldName
	 * @return
	 */
	public String[] getSourceTransformationMethod(String targetFieldName) {
		FieldMapping fieldMap=new FieldMapping();
		for (int i = 0; i < fieldMapping.length; i++) {
			
			String fieldMappingString = fieldMapping[i];
			fieldMap.setFieldMapping(fieldMappingString);
			
			if(fieldMap.hasTargetField(targetFieldName))
			{
				return fieldMap.getTransformations();
			}
		}
		return null;
	}

	/**
	 * Context lookup methods for Pull Connector to read,
	 * get the target field when source field name is available
	 * @param sourceFieldName
	 * @return
	 */
	public String getTargetField(String sourceFieldName) {
		FieldMapping fieldMap=new FieldMapping();
		for (int i = 0; i < fieldMapping.length; i++) {
			
			String fieldMappingString = fieldMapping[i];
			fieldMap.setFieldMapping(fieldMappingString);
			
			if(fieldMap.hasSourceField(sourceFieldName))
			{
				return fieldMap.getTargetField();
			}
		}
		
		return null;
	}

	/**
	 * Context lookup methods for Pull Connector to read,
	 * get the filter expression when source field name is available
	 * @param sourceFieldName
	 * @return
	 */
	public String[] getTargetFilter(String sourceFieldName) {
		FieldMapping fieldMap=new FieldMapping();
		for (int i = 0; i < fieldMapping.length; i++) {
			
			String fieldMappingString = fieldMapping[i];
			fieldMap.setFieldMapping(fieldMappingString);
			
			if(fieldMap.hasSourceField(sourceFieldName))
			{
				return fieldMap.getFilters();
			}
		}
		return null;
	}

	/**
	 * Context lookup methods for Pull Connector to read,
	 * get the transformation method when source field name is available
	 * @param sourceFieldName
	 * @return
	 */
	public String[] getTargetTransformationMethod(String sourceFieldName) {
		FieldMapping fieldMap=new FieldMapping();
		for (int i = 0; i < fieldMapping.length; i++) {
			
			String fieldMappingString = fieldMapping[i];
			fieldMap.setFieldMapping(fieldMappingString);
			
			if(fieldMap.hasSourceField(sourceFieldName))
			{
				return fieldMap.getTransformations();
			}
			
		}
		return null;
	}

	/**
	 * Context lookup for ConnectorConfigurationString[]
	 * @return
	 */
	public String[] getConnectorConfiguration() {
		return this.connectorConfiguration;
	}

	/**
	 * Context lookup for fieldMapping
	 * @return
	 */
	public String[] getFieldMapping() {		
		return this.fieldMapping;
	}

}