package com.datatools.DataTransporter;

public class OneTimeWriteStrategy extends AbstractConnectorWriteStrategy {

	Logger log=new Logger().getLogger(this.getClass());
	public OneTimeWriteStrategy(Connector connector) {
		super(connector);
	}



	@Override
	public void doFilterAndWrite() throws ConnectorException {
//		log.info("filterAndWrite called");
		connector.doFilterAndWrite();
		
	}

}