package com.datatools.DataTransporter

/**
 * Configuration Class for fields in a Connector
 * @author Vinod Panicker
 *
 */
class ConnectorFieldCfg {
	
	//Field Name
	String name
	//DataType
	String dataType
	//character size of the Field
	String fieldWidth
	//order of the field 
	String order
	
	public ConnectorFieldCfg()
	{
	}
	
	public ConnectorFieldCfg(String name,String dataType,String fieldWidth,String order) {
		this.name=name
		this.dataType=dataType
		this.fieldWidth=fieldWidth
		this.order=order
		
	}
	public ConnectorFieldCfg(String name,String dataType,String fieldWidth) {
		this.name=name
		this.dataType=dataType
		this.fieldWidth=fieldWidth
		
	}
	
	public ConnectorFieldCfg(String name,String dataType) {
		this.name=name
		this.dataType=dataType
	}
	
	public ConnectorFieldCfg(String name) {
		this.name=name
		
	}
	
}