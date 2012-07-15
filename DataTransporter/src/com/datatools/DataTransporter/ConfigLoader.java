package com.datatools.DataTransporter;

public interface ConfigLoader {
	
	//Loads the schedule configuration for a Connection even before the rest of the connection configuration is loaded
	public void loadScheduleConfiguration(Connection connection);

	public  void loadConnectionConfiguration(Connection connection);
	
	public  void loadConnectorConfiguration(Connector connector);

}