package com.datatools.DataTransporter


/**
 * Configuration Class for a connector
 * @author Vinod Panicker
 *
 */
class ConnectorCfg{
	
	def name
	def type
	def mode
	def parameters
	static Logger log=new Logger().getLogger(this.getClass());
	
	public ConnectorCfg() {
	}
	
	public ConnectorCfg(String type, String mode, String name,HashMap parameters) {
		this(type,mode,name)
		this.parameters=parameters
	}
	
	public ConnectorCfg(String type, String mode, String name) {
		this.type=type
		this.mode=mode
		this.name=name
	}
	
	public String print()
	{
//		return name+type+mode+parameters
		log.debug(name+type+mode+parameters)
	}
	
}