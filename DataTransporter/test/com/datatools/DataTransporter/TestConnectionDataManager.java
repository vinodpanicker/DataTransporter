package com.datatools.DataTransporter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * TestConnectionDataManager can be used to test and simulate connections
 * 
 * @author Vinod Panicker
 *
 */
public class TestConnectionDataManager {

	Logger log=new Logger().getLogger(this.getClass());
	private static HashMap<String, TestConnectionDataGenerator> generators = new HashMap<String, TestConnectionDataGenerator>();
	private static HashMap<String, Serializable> generatedData = new HashMap<String, Serializable>();

	public static TestConnectionDataGenerator getDataGenerator(
			String connectionName) {

		if (generators.containsKey(connectionName)) {
			return generators.get(connectionName);
		}
		
		TestConnectionDataGenerator testConnectionDataGenerator = new TestConnectionDataGenerator(connectionName);
		generators.put(connectionName, testConnectionDataGenerator);
		generatedData.put(connectionName, new ArrayList());
		return testConnectionDataGenerator;
	}

	public static int getNoOfRecordsGenerated(String connectionName) {

		int noOfRecords = 0;

		if (generators.containsKey(connectionName)) {

			return getNoOfRecordsInGeneratorData(connectionName);
		} else {
			return noOfRecords;
		}
	}

	private static int getNoOfRecordsInGeneratorData(String connectionName) {
		if (generatedData.containsKey(connectionName)) {
			return ((ArrayList) generatedData.get(connectionName)).size();
		} else
			return 0;
	}

	public static void setGeneratedData(String connectionName, ArrayList data) {
		if (generatedData.containsKey(connectionName)) {
			generatedData.put(connectionName, data);
		} else {
			System.out
					.println("Connection Not found in TestConnectionDataManager "
							+ connectionName);
			// connection not found
		}

	}

	public static void addData(String connectionName, ArrayList data) {
		if (generatedData.containsKey(connectionName)) {
			ArrayList existingdata = (ArrayList) generatedData
					.get(connectionName);
			if (existingdata == null) {
				generatedData.put(connectionName, data);
			} else {
				existingdata.addAll(data);
				generatedData.put(connectionName, existingdata);

			}
		} else {
			System.out
					.println("Connection Not found int TestConnectionDataManager :"
							+ connectionName);
			// connection not found
		}

	}

	public static ArrayList getRecords(String connectionName, boolean remove) {
		ArrayList existingdata=null;
		
		if (generatedData.containsKey(connectionName)) {
			existingdata = (ArrayList) generatedData
					.get(connectionName);
		}
		
		if(remove)
		{
			generatedData.put(connectionName, new ArrayList());
		}
		return existingdata;
	}
	

}