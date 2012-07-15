package com.datatools.DataTransporter;

/**
 * 
 * @author Vinod Panicker
 *  Basic default Strategy for on time straight through read.
 *  //TODO  Integrate with the Connector
 */

public class OneTimeReadStrategy extends AbstractConnectorReadStrategy {

	Logger log= new Logger().getLogger(this.getClass());
	
	public OneTimeReadStrategy(Connector connector) {
		super(connector);
	
	}

	@Override
	public void doReadAndTransform() throws ConnectorException {
//		log.info("readAndTransform called");
		connector.doReadAndTransform();

	}



}