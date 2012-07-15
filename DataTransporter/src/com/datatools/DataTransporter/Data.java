package com.datatools.DataTransporter;

import java.util.List;


public interface Data {

	public abstract Object getRawData();

	// return true if the task was successfully placed on the queue, false
	// if the queue has been shut down.
	public abstract void addRecord(List record);

	// return the head task from the queue, or null if no task is available
	public abstract List getRecord();
	
	public abstract List recoverRecords();

	public abstract int count();

	public abstract void setRawData(Object rawData);

}