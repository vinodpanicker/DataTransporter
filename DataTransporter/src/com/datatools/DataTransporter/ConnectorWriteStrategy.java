package com.datatools.DataTransporter;

public interface ConnectorWriteStrategy {

	void doFilterAndWrite() throws ConnectorException;

}