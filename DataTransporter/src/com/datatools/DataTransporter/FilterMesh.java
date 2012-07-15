package com.datatools.DataTransporter;

import java.util.HashMap;
import java.util.List;

public interface FilterMesh {

	boolean filter(String name,
			List<ConnectorFieldMapping> connectorFieldMappings, HashMap inRecord);

}