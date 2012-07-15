package com.datatools.DataTransporter;

import java.util.HashMap;
import java.util.List;

public class DefaultFilterMesh implements FilterMesh{

	@Override
	public boolean filter(String name,List<ConnectorFieldMapping> connectorFieldMappings, HashMap inRecord) {
		// TODO need to add condition here to know what records should be filtered out
		return true;
	}

}