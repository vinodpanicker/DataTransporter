package com.datatools.DataTransporter

import java.text.DateFormat
import java.text.SimpleDateFormat

/**
 * 
 * Class used to generate Test Data
 * 
 * Must be enhanced further to generate data with different data type an methods
 * @author Vinod Panicker
 *
 */
public class TestConnectionDataGenerator {
	Logger log=Logger.getLogger(TestConnectionDataGenerator.class);
	public static final boolean REMOVE = true;
	private String connectionName;
	private int recordCount=0;
	public ArrayList data = new ArrayList();
	public ArrayList records= new ArrayList();
	public TestConnectionDataGenerator(String connectionName) {
		this.connectionName=connectionName;
	}
	
	/**
	 * This method is used to simulate a connection
	 * @param noOfRecords
	 */
	public void generateDataRecords(int noOfRecords) {
		String pullConnectorName = getPullconnectorName();
		log.info "Generating Data Records for :"+pullConnectorName
		HashMap connectorFields = ConnectorFactory.getConnectorFields(pullConnectorName);
		
		ArrayList data=generateDataRecordsFromConnectorFields(connectorFields,noOfRecords);
		TestConnectionDataManager.addData(connectionName,data);
	}
	
	private String getPullconnectorName() {		
		return ConnectorFactory.getConnectorName(connectionName,Connector.MODE_PULL);
	}
	
	private ArrayList generateDataRecordsFromConnectorFields(
	HashMap connectorFields, int noOfRecords) {
		
		log.info "No of connectorFields:"+ connectorFields.size()
		for(int i=recordCount;i<(noOfRecords+recordCount);i++) {
			HashMap record = new HashMap()
			connectorFields.each{ 
				String connectorFieldName=it.key
				ConnectorFieldCfg  connectorFieldCfg =it.value
				Object data= generateFieldData(connectorFieldCfg,i)
				record.put(connectorFieldName,data);
			}
			records << record;
		}
		
		TestConnectionHelper.printContent(records,"Records Created");
		this.recordCount=noOfRecords;
		return records;
	}
	
	/**
	 * To Extend support for more data types
	 * @param connectorFieldCfg
	 * @param recordNo
	 * @return
	 */
	private Object generateFieldData(ConnectorFieldCfg connectorFieldCfg,int recordNo)
	{
		String connectorFieldName=connectorFieldCfg.name
		String connectorFieldType=connectorFieldCfg.dataType
		
		def data=""
		switch(connectorFieldType.toLowerCase())
		{
			case "string":
				data=connectorFieldName+"data"+recordNo
				break
			case "int":
				data=100+recordNo
				break
			case "date":
				data=new Date()
				DateFormat dateFormat= new SimpleDateFormat("yyyy-MM-dd");
				data = dateFormat.format(data);
				break	
			
			default:
				data=connectorFieldName+"data"+recordNo;
		}
		
		return data;
	}
	public void generateMoreDataRecords(int batchSize) {
		generateDataRecords(batchSize)
		
	}
	
	public ArrayList getRecords() {
		
		return TestConnectionDataManager.getRecords(connectionName,REMOVE);
	}	
}