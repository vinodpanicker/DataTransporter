package com.datatools.DataTransporter

public class TransformationHelper {
	
	static def invokeTranformation(def methodName,def args,def connectorType){
		// <TODO> Need to see if, there is a way to query all the classes ending with 'Transformation'
		def transformationClass = DataTransporterConstants.TRANSFORMATION_NAMESPACE+"."+connectorType+ "Transformation"
		return Class.forName(transformationClass).newInstance()."$methodName"(args);
		
	}
	
	
//	public static String invokeTranformation(String methodName,
//	def args,def connectorType) {
//		def transformationClass = DataTransporterConstants.TRANSFORMATION_NAMESPACE+"."+connectorType+ "Transformation"
//		return Class.forName(transformationClass).newInstance()."$methodName"(args);
//	}
}