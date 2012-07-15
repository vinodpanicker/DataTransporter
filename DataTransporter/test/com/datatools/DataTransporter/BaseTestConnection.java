package com.datatools.DataTransporter;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Assert;

public class BaseTestConnection extends AbstractTestConnection {

	private ArrayList<ConnectorCfg> connectorCfgs = new ArrayList<ConnectorCfg>();
	private ArrayList<FieldMappingCfg> fieldMappingCfgs = new ArrayList<FieldMappingCfg>();
	protected Connection testConnection;
	private ArrayList<ScheduleCfg> scheduleCfgs;
	protected ConnectionManager connectionManager = new ConnectionManager();

	protected ArrayList<ScheduleCfg> setupScheduleCfgs() {
		String workDays = "Calendar.MONDAY..Calendar.FRIDAY";
		String officeHours = "8..18";

		// String workDays = "MONDAY";
		// String officeHours = "8";

		String startEveryXMinutes = "1";
		String startEveryXHours = "1";
		return addScheduleCfg(workDays, officeHours, startEveryXMinutes,
				startEveryXHours);
	}

	private ArrayList<ScheduleCfg> addScheduleCfg(String workDays,
			String officeHours, String startEveryXMinutes,
			String startEveryXHours) {
		ArrayList<ScheduleCfg> scheduleCfgs = new ArrayList<ScheduleCfg>();
		ScheduleCfg scheduleCfg1 = new ScheduleCfg(workDays, officeHours,
				startEveryXMinutes, startEveryXHours);
		scheduleCfgs.add(scheduleCfg1);
		return scheduleCfgs;
	}

	protected ArrayList<FieldMappingCfg> setupFieldMappingCfg() {
		ArrayList<FieldMappingCfg> fieldMappingCfgs = new ArrayList<FieldMappingCfg>();
		setupFieldMappingCfg(fieldMappingCfgs);
		return fieldMappingCfgs;
	}

	protected ArrayList<ConnectorCfg> setupConnectorCfgs() {
		ConnectorCfg connectorCfg1 = setupConnectorCfg1();
		ConnectorCfg connectorCfg2 = setupConnectorCfg2();
		ArrayList<ConnectorCfg> connectorCfgs = new ArrayList<ConnectorCfg>();
		connectorCfgs.add(connectorCfg1);
		connectorCfgs.add(connectorCfg2);
		return connectorCfgs;
	}

	protected ConnectorCfg setupConnectorCfg2() {
		HashMap<String, String> additionalParameters2 = new HashMap<String, String>();
		additionalParameters2.put("tablename", "connections");
		ConnectorCfg connectorCfg2 = new ConnectorCfg("TestHbaseConnector",
				"PUSH", "TestInternalDataStoreHbasePushConnector",
				additionalParameters2);
		return connectorCfg2;
	}

	protected ConnectorCfg setupConnectorCfg1() {
		HashMap<String, String> additionalParameters1 = new HashMap<String, String>();
		additionalParameters1.put("tablename", "input");
		additionalParameters1.put("families", "SourceField3");
		additionalParameters1.put("qualifiers", "SourceField3:qualifier2");
		additionalParameters1.put("qualifiers", "SourceField3:qualifier1");
		additionalParameters1.put("resources", "111");

		ConnectorCfg connectorCfg1 = new ConnectorCfg("TestHbaseConnector",
				"PULL", "TestHbasePullConnector", additionalParameters1);
		return connectorCfg1;
	}

	protected void setupConnectorCfg2(
			ArrayList<ConnectorFieldCfg> connectorFieldCfgs2) {
		ConnectorFieldCfg connectorFieldCfg4 = new ConnectorFieldCfg(
				"TargetField1", "String", "10", "1");
		ConnectorFieldCfg connectorFieldCfg5 = new ConnectorFieldCfg(
				"TargetField2", "String", "10", "2");
		ConnectorFieldCfg connectorFieldCfg6 = new ConnectorFieldCfg(
				"TargetField3", "String", "10", "3");
		connectorFieldCfgs2.add(connectorFieldCfg4);
		connectorFieldCfgs2.add(connectorFieldCfg5);
		connectorFieldCfgs2.add(connectorFieldCfg6);
	}

	protected void setupConnectorCfg1(
			ArrayList<ConnectorFieldCfg> connectorFieldCfgs1) {
		ConnectorFieldCfg connectorFieldCfg1 = new ConnectorFieldCfg(
				"SourceField1", "String", "10", "1");
		ConnectorFieldCfg connectorFieldCfg2 = new ConnectorFieldCfg(
				"SourceField2", "String", "10", "2");
		ConnectorFieldCfg connectorFieldCfg3 = new ConnectorFieldCfg(
				"SourceField3", "String", "10", "3");
		connectorFieldCfgs1.add(connectorFieldCfg1);
		connectorFieldCfgs1.add(connectorFieldCfg2);
		connectorFieldCfgs1.add(connectorFieldCfg3);
	}

	protected void setupFieldMappingCfg(
			ArrayList<FieldMappingCfg> fieldMappingCfgs) {
		FieldMappingCfg fieldMappingCfg1 = new FieldMappingCfg("SourceField1",
				"TargetField1", new String[] { "Filter1" },
				new String[] { "TransformationMethod1" }, "FieldMappingName1");
		FieldMappingCfg fieldMappingCfg2 = new FieldMappingCfg("SourceField2",
				"TargetField2", new String[] { "Filter2" },
				new String[] { "TransformationMethod2" }, "FieldMappingName2");
		FieldMappingCfg fieldMappingCfg3 = new FieldMappingCfg("SourceField3",
				"TargetField3", new String[] { "Filter3" },
				new String[] { "TransformationMethod3" }, "FieldMappingName3");
		fieldMappingCfgs.add(fieldMappingCfg1);
		fieldMappingCfgs.add(fieldMappingCfg2);
		fieldMappingCfgs.add(fieldMappingCfg3);
	}

	protected void createPullConnectors(String connectorType,
			String connectorName, int numberOfFields, String fieldPrefix) {
		String mode = "PULL";
		createConnector(connectorType, connectorName, numberOfFields,
				fieldPrefix, mode);
	}

	protected void createPullConnectors(String connectorType,
			String connectorName, String[] fieldNames) {
		String mode = "PULL";
		createConnector(connectorType, connectorName, fieldNames, mode);
	}

	protected void createPullConnectors(String connectorType,
			String connectorName, String[] fieldNames, String[] dataTypes) {
		String mode = "PULL";
		createConnector(connectorType, connectorName, fieldNames, dataTypes,
				mode);
	}

	/**
	 * @param connectorType
	 * @param connectorName
	 * @param numberOfFields
	 * @param fieldPrefix
	 * @param mode
	 */
	protected void createConnector(String connectorType, String connectorName,
			int numberOfFields, String fieldPrefix, String mode) {
		HashMap<String, String> additionalParameters1 = new HashMap<String, String>();
		additionalParameters1.put("tablename", "input");
		additionalParameters1.put("families", "SourceField3");
		additionalParameters1.put("qualifiers", "SourceField3:qualifier2");
		additionalParameters1.put("qualifiers", "SourceField3:qualifier1");
		additionalParameters1.put("resources", "111");
		createConnectorWithParams(connectorType, connectorName, numberOfFields,
				fieldPrefix, mode, additionalParameters1);
	}

	/**
	 * 
	 * @param connectorType
	 * @param connectorName
	 * @param numberOfFields
	 * @param fieldPrefix
	 * @param mode
	 * @param params
	 */
	private void createConnectorWithParams(String connectorType,
			String connectorName, int numberOfFields, String fieldPrefix,
			String mode, HashMap params) {
		connectorCfgs.add(TestConnectionHelper
				.setupDefaultConnectorCfgWithParams(connectorType, mode,
						connectorName, params));

		ArrayList<ConnectorFieldCfg> connectorFieldCfgs = TestConnectionHelper
				.setupDefaultConnectorFieldCfg(numberOfFields, fieldPrefix);
		Assert.assertTrue(ConfigurationHelper.createConnectorCfgFileFromFieldCfgs(
				connectorName, connectorFieldCfgs));
		// Test if connector has 10 fields created
		Assert.assertEquals(numberOfFields, ConnectorFactory.getConnectorFields(
				connectorName).size());
	}

	/**
	 * @param connectorType
	 * @param connectorName
	 * @param numberOfFields
	 * @param fieldPrefix
	 * @param mode
	 */
	private void createConnector(String connectorType, String connectorName,
			String[] fieldNames, String mode) {
		connectorCfgs.add(TestConnectionHelper.setupDefaultConnectorCfg(mode,
				connectorType, connectorName));

		ArrayList<ConnectorFieldCfg> connectorFieldCfgs = TestConnectionHelper
				.setupDefaultConnectorFieldCfg(fieldNames);
		Assert.assertTrue(ConfigurationHelper.createConnectorCfgFileFromFieldCfgs(
				connectorName, connectorFieldCfgs));
		// Test if connector has 10 fields created
		Assert.assertEquals(fieldNames.length, ConnectorFactory.getConnectorFields(
				connectorName).size());
	}

	private void createConnector(String connectorType, String connectorName,
			String[] fieldNames, String[] dataTypes, String mode) {
		ConnectorCfg ConnectorCfgWithParams = TestConnectionHelper
				.setupDefaultConnectorCfg(mode, connectorType, connectorName);
		createConnector(connectorName, fieldNames, dataTypes,
				ConnectorCfgWithParams);
	}

	/**
	 * @param connectorType
	 * @param connectorName
	 * @param fieldNames
	 * @param dataTypes
	 * @param mode
	 * @param parameters
	 */

	private void createConnectorsWithParams(String connectorType,
			String connectorName, String[] fieldNames, String[] dataTypes,
			String mode, HashMap<String, String> parameters) {
		ConnectorCfg ConnectorCfgWithParams = TestConnectionHelper

				.setupDefaultConnectorCfgWithParams(connectorType,mode, 
						connectorName, parameters);
		createConnector(connectorName, fieldNames, dataTypes,
				ConnectorCfgWithParams);
	}

	/**
	 * @param connectorName
	 * @param fieldNames
	 * @param dataTypes
	 * @param ConnectorCfgWithParams
	 */
	private void createConnector(String connectorName, String[] fieldNames,
			String[] dataTypes, ConnectorCfg ConnectorCfgWithParams) {
		connectorCfgs.add(ConnectorCfgWithParams);

		ArrayList<ConnectorFieldCfg> connectorFieldCfgs = TestConnectionHelper
				.setupDefaultConnectorFieldCfg(fieldNames, dataTypes);
		Assert.assertTrue(ConfigurationHelper.createConnectorCfgFileFromFieldCfgs(
				connectorName, connectorFieldCfgs));
		// Test if connector has 10 fields created
		Assert.assertEquals(fieldNames.length, ConnectorFactory.getConnectorFields(
				connectorName).size());
	}

	/**
	 * @deprecated Use {@link #createPushConnectors(String,String,int,String)}
	 *             instead
	 */
	protected void createPushConnectors(String connectorType,
			String connectorName, String fieldPrefix, int numberOfFields) {
		createPushConnectors(connectorType, connectorName, numberOfFields,
				fieldPrefix);
	}

	/**
	 * test Method to create Push connector with connectorType, ConnectorName,
	 * NumberOfFields and prefix for the fieldName
	 * 
	 * @param connectorType
	 * @param connectorName
	 * @param numberOfFields
	 * @param fieldPrefix
	 */
	protected void createPushConnectors(String connectorType,
			String connectorName, int numberOfFields, String fieldPrefix) {
		String mode = "PUSH";
		createConnector(connectorType, connectorName, numberOfFields,
				fieldPrefix, mode);
	}

	protected void createPushConnectors(String connectorType,
			String connectorName, String[] fieldNames) {
		String mode = "PUSH";
		createConnector(connectorType, connectorName, fieldNames, mode);
	}

	protected void createPushConnectors(String connectorType,
			String connectorName, String[] fieldNames, String[] dataTypes) {
		String mode = "PUSH";
		createConnector(connectorType, connectorName, fieldNames, dataTypes,
				mode);
	}

	/**
	 * Method creates
	 * 
	 * @param numberOfFieldsMappings
	 * @param connectionName
	 */
	protected void createConnection(int numberOfFieldsMappings,
			String connectionName) {
		fieldMappingCfgs = TestConnectionHelper
				.setupDefaultFieldMappingCfgs(numberOfFieldsMappings);

		scheduleCfgs = TestConnectionHelper.setupDefaultScheduleCfgs();
		Assert.assertNotNull(ConfigurationHelper.getConnectionConfigurationAsString(
				connectionName, connectorCfgs, fieldMappingCfgs, scheduleCfgs));
		Assert.assertTrue(ConfigurationHelper.createConnectionCfgFile(connectionName,
				connectorCfgs, fieldMappingCfgs, scheduleCfgs));

		testConnection = ConnectionManager.getConnection(connectionName);
		Assert.assertNotNull(testConnection);
	}

	protected void createConnection(String connectionName,
			String[] sourceFieldNames, String[] targetFieldNames,
			String[] filters, String[] transformations) {
		fieldMappingCfgs = TestConnectionHelper.setupFieldMappingCfgs(
				sourceFieldNames, targetFieldNames, filters, transformations);

		scheduleCfgs = TestConnectionHelper.setupDefaultScheduleCfgs();
		Assert.assertNotNull(ConfigurationHelper.getConnectionConfigurationAsString(
				connectionName, connectorCfgs, fieldMappingCfgs, scheduleCfgs));
		Assert.assertTrue(ConfigurationHelper.createConnectionCfgFile(connectionName,
				connectorCfgs, fieldMappingCfgs, scheduleCfgs));

		testConnection = ConnectionManager.getConnection(connectionName);
		Assert.assertNotNull(testConnection);

	}

	/**
	 * Test method to validate configuration files take <connectionfileName> eg
	 * Testconnection.con as input
	 * 
	 * @param filename
	 */
	protected void validateConfigurationFiles(String filename) {
		File connectionCfgFile = new File("./.config/" + filename);

		ArrayList schedule1 = ScheduleHelper
				.getSchedulesfromScheduleCfg(ConfigurationHelper
						.getScheduleCfg(connectionCfgFile));

		// Object workDays1 = ((Schedule)schedule1.get(0)).getWorkDays();
		// System.out.println(workDays1);
		//        
		// Object workDays2 =
		// ((Schedule)(testConnection.getSchedules().get(0))).getWorkDays();
		// System.out.println(workDays2);
		//        
		// assertEquals(workDays1,workDays2);

		Assert.assertEquals(((Schedule) schedule1.get(0)).getWorkDays(),
				testConnection.getSchedules().get(0).getWorkDays());

		Assert.assertEquals(DebugConfigHelper.print(ConfigurationHelper
				.getConnectorsCfg(connectionCfgFile)), DebugConfigHelper
				.print(connectorCfgs));
		// assertEquals(DebugConfigHelper.print(ConfigurationHelper.getFieldMappingCfg(connectionCfgFile)),
		// DebugConfigHelper.print(fieldMappingCfgs));
	}

	public void setupDataToBeRead(String connectionName, int noOfRecords) {
		TestConnectionDataGenerator testConnectionDataGenerator = TestConnectionDataManager
				.getDataGenerator(connectionName);

		testConnectionDataGenerator.generateDataRecords(noOfRecords);
		Assert.assertEquals(noOfRecords, TestConnectionDataManager.getNoOfRecordsGenerated(connectionName));
	}

	/**
	 * @param connectionName
	 */
	protected void activateConnection(String connectionName) {
		connectionManager.startConnection(connectionName);

		Assert.assertTrue(connectionManager.hasActiveConnections());
		while (connectionManager.hasActiveConnections()) {
			// int i = 0;
			// try {
			// i = System.in.read();
			// } catch (IOException e) {
			// e.printStackTrace();
			// }
			//	
			// if (i != 0) {
			// break;
			// }
		}
		// assertEquals(ConnectionStatus.CONNECTION_ACTIVATED, testConnection
		// .getStatus());
	}

	/**
	 * @param connectionName
	 * @param millis
	 */
	protected void activateConnectionAndWait(String connectionName, long millis) {
		connectionManager.startConnection(connectionName);

		Assert.assertTrue(connectionManager.hasActiveConnections());

		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

	}

	protected void createPushConnectorsWithParams(String connectorType,
			String connectorName, String[] fieldNames, String[] dataTypes,
			HashMap<String, String> additionalParameters) {
		String mode = "PUSH";
		createConnectorsWithParams(connectorType, connectorName, fieldNames,
				dataTypes, mode, additionalParameters);
	}

	protected void createPullConnectorsWithParams(String connectorType,
			String connectorName, String[] fieldNames, String[] dataTypes,
			HashMap<String, String> parameters) {

		String mode = "PULL";
		createConnectorsWithParams(connectorType, connectorName, fieldNames,
				dataTypes, mode, parameters);
	}

	protected synchronized void deactivateConnection() {
		
		connectionManager.stopAllConnections();
	
	}

	protected void removeConnection(String connectionName) {
		connectionManager.removeConnection(connectionName);
		 connectorCfgs = new ArrayList<ConnectorCfg>();
		 fieldMappingCfgs = new ArrayList<FieldMappingCfg>();
	}

}