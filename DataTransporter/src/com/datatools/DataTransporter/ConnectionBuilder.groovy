package com.datatools.DataTransporter


import java.util.Map;

import groovy.util.BuilderSupport;

class ConnectionBuilder extends BuilderSupport {

	def result =''<<''
	
	def createNode(name) {
		
		return check(name)
	}

	def createNode(name,value) {
		return check(createNode(name) << format(value))
	}

	def createNode(name, Map attributes) {
		return check(createNode(name) << format(attributes))
	}

	def createNode(name, Map attributes,value) {
		return check(createNode(name,attributes) << format(value))
	}

	void setParent(parent,child) {

		result << child.toString()<<","
		
	}
	
	void nodeCompleted(parent,node){
		if(parent==null)
			{	
			// remove the last comma
			result.deleteCharAt(result.length()-1)
			// add new line
			result << "\n"
			}
		}
	
	private check(descr)
	{
		// prints parent node 
		if(!current) result << descr +"="
		return descr
	}
	
	private format(value){
		return ':' <<"'"+ value.toString()+"'"
	}
	
	private format(Map attributes)
	{
		StringBuffer formatted =''<< '['
		attributes.each{ key,value ->
		  formatted << key << ':' << value <<', '
		}
		formatted.length= formatted.size()-2
		formatted <<']'
		return formatted
	}

}