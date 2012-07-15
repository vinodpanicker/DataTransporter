package com.datatools.DataTransporter

/**
 * Configuration Class for FieldMapping
 * @author Vinod Panicker
 *
 */
class FieldMappingCfg{
	
	def name
	def source
	def target
	String[] filters
	String[] transformations
	static Logger log = DataTransporter.log;
	
	public FieldMappingCfg()
	{}
	
	public FieldMappingCfg(String sourceFieldName, String targetFieldName, String[] filterName,
	String[] transformationMethodNames, String fieldMappingName) {
		this.name=fieldMappingName
		this.source=sourceFieldName
		this.target=targetFieldName
		this.filters=filterName
		this.transformations=transformationMethodNames
		
	}
	
	public FieldMappingCfg(String sourceFieldName, String targetFieldName, def filterName,
			def transformationMethodNames, String fieldMappingName) {
				this.name=fieldMappingName
				this.source=sourceFieldName
				this.target=targetFieldName
				this.filters=filterName
				this.transformations=transformationMethodNames
				
			}
	
	/**
	 * Used only for testing
	 * @return
	 */
	public String print()
	{
//		return name+source+target+filters+transformations
		log.debug(name+source+target+filters+transformations)
	}
}
