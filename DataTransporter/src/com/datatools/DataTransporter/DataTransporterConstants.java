package com.datatools.DataTransporter;

import java.util.Calendar;


public class DataTransporterConstants {

	public static  String CONFIGFILE_LOCATION = "./.config/";
	public static  String LOG4JFILE_LOCATION = "./"; 
	public static final String FIELDMAPPING_PREFIX = "fieldMapping=";
	public static final String CONNECTOR_PREFIX = "connector=";
	public static final String CONNECTOR_NAMESPACE = "com.datatools.datatransporter.connector";
	public static final String TRANSFORMATION_NAMESPACE = "com.datatools.datatransporter.connector.transformation";
	public static final String IN_MEMORY_DATASTORE = "In Memory Data Store";
	public static final String INTERFACE_XLS_LOCATION = "./xls/";
	public static final int SCHEDULER_DEFAULT_FIRST_WORK_DAY = Calendar.MONDAY;
	public static final int SCHEDULER_DEFAULT_LAST_WORK_DAY = Calendar.FRIDAY;
	public static final int SCHEDULER_DEFAULT_START_OFFICE_HOURS = 8;
	public static final int SCHEDULER_DEFAULT_END_OFFICE_HOURS = 18;
	public static final int SCHEDULER_DEFAULT_FOR_EVERY_XMINUTES = 10;
	public static final int SCHEDULER_DEFAULT_FOR_EVERY_XHOURS = 1;
	public static final String CONNECTION_FILE_EXTENSION = "Connection.con";
	public static final int SCHEDULER_DEFAULT_DELAY = 10;
	public static final String SCHEDULE_PREFIX = "schedule=";
	public static final int DEFAULT_READ_POLL_DELAY_IN_SECONDS = 3;
	public static final String ALLOW_MULTIPLE_CONNECTIONS="No";

}