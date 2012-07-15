package com.datatools.DataTransporter;

import java.util.HashMap;
import java.util.List;


/**
 * The Default transformer Class
 * 
 * @author Vinod Panicker
 * 
 */
public class DefaultTransformer implements Transformer {

	Logger log=new Logger().getLogger(this.getClass());
	/**
	 * This method uses the connectorName to identify the transformation class
	 */

	public HashMap<String, Object> transform(String connectorName,
			List<ConnectorFieldMapping> connectorFieldMappings,
			HashMap<String, Object> inRecord) {
		HashMap<String, Object> outRecord = new HashMap<String, Object>();

		for (ConnectorFieldMapping connectorfieldMapping : connectorFieldMappings) {
			String field = connectorfieldMapping.getConnectionFieldMappingName();
			String transformationMethodNames[] = connectorfieldMapping
					.getTransformations();
			outRecord.put(field, transform(connectorName,
					transformationMethodNames, (Object) inRecord.get(field)));
		}

		return outRecord;
	}

	/**
	 * This method calls all Transformations on a field one after the another on
	 * the field till the desired value is got
	 * 
	 * @param connectorName
	 * @param transformationMethodNames
	 * @param infieldValue
	 * @return
	 */
	private Object transform(String connectorName,
			String[] transformationMethodNames, Object infieldValue) {
		Object outFieldValue = infieldValue;
		
		if (transformationMethodNames == null||transformationMethodNames.length==0)
			return outFieldValue;
		
		for (String transformationMethodName : transformationMethodNames) {
			outFieldValue = transform(connectorName, transformationMethodName,
					outFieldValue);
		}
		return outFieldValue;
	}

	// transform the value of a single field
	private Object transform(String connectorName,
			String transformationMethodName, Object infieldValue) {
		Object outFieldValue = infieldValue;
		if (transformationMethodName == null||transformationMethodName=="")
			return outFieldValue;
        try{
		outFieldValue = (Object) TransformationHelper.invokeTranformation(
				transformationMethodName, infieldValue, connectorName);
        }catch(Exception e)
        {
        	log.error("InValid transformation !!:["+transformationMethodName +"]Skipping!");
        	return outFieldValue;
        }
		
		return outFieldValue;
	}

}