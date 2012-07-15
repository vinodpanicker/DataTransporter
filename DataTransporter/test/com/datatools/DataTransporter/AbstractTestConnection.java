package com.datatools.DataTransporter;

import java.util.ArrayList;
import java.util.HashMap;

public class AbstractTestConnection {

	public AbstractTestConnection() {
		super();
	}

	protected ArrayList<ScheduleCfg> setupScheduleCfgs() {
		//String workDays = "Calendar.MONDAY..Calendar.FRIDAY";
		//String officeHours = "8..18";
	
		String workDays = "MONDAY";
		String officeHours = "8";
	
		String startEveryXMinutes = "1";
		String startEveryXHours = "1";
		return setupScheduleCfg(workDays, officeHours, startEveryXMinutes,
				startEveryXHours);
	}

	private ArrayList<ScheduleCfg> setupScheduleCfg(String workDays,
			String officeHours, String startEveryXMinutes,
			String startEveryXHours) {
		ArrayList<ScheduleCfg> scheduleCfgs = new ArrayList<ScheduleCfg>();
//		ScheduleCfg scheduleCfg1 = new ScheduleCfg(
//				workDays, officeHours, startEveryXMinutes, startEveryXHours);
//		scheduleCfgs.add(scheduleCfg1);
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

	protected void setupConnectorCfg2(ArrayList<ConnectorFieldCfg> connectorFieldCfgs2) {
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

	protected void setupConnectorCfg1(ArrayList<ConnectorFieldCfg> connectorFieldCfgs1) {
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

	protected void setupFieldMappingCfg(ArrayList<FieldMappingCfg> fieldMappingCfgs) {
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

}