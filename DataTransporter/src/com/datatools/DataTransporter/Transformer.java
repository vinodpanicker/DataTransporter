package com.datatools.DataTransporter;

import java.util.HashMap;
import java.util.List;

public interface Transformer {

	public HashMap<String,Object> transform(String connectorName, List<ConnectorFieldMapping> connectorFieldMappings,HashMap<String,Object> inRecord);

}