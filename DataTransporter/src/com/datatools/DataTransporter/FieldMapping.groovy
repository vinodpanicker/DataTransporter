package com.datatools.DataTransporter

public class FieldMapping {

	String name;
	String sourceField;
	String targetField;
	String[] filters;
	String[] transformations;
	

	
	//name:'EMPLOYEENO',source:'EMPNO',target:'EMPNO',filters:'~=/E',transformation:'toNumerics()'
	
	public void setFieldMapping(String fieldMappingString) {
		GroovyShell gs= new GroovyShell(); 
		String expression="["+fieldMappingString+"]"
		def result=gs.evaluate(expression)
        
		name=result.get("name")
		filters=result.get("filters")
		sourceField=result.get("source")
		targetField=result.get("target")
		transformations=result.get("transformation")
		
		
	}
	/**
	 * Method the check if expected targetFieldName is present in the fieldmapping.
	 * @param targetFieldName
	 * @return
	 */
	public boolean hasTargetField(String targetFieldName) {
		return getTargetField()?.equals(targetFieldName)
	}
	
	/**
	 * Method to check if expected sourceFieldname is present in the fieldmapping.
	 * @param sourceFieldName
	 * @return
	 */
	public boolean hasSourceField(String sourceFieldName) {
		return getSourceField()?.equals(sourceFieldName)
	}


	
}