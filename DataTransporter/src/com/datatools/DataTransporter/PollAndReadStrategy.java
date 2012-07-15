package com.datatools.DataTransporter;

/**
 * 
 * @author Vinod Panicker
 *  
 *  To be implemented and integrated later with connector
 *  while connector is refactored to remove large class smell from connector 
 *
 */
public class PollAndReadStrategy extends AbstractConnectorReadStrategy {

	Logger log=new Logger().getLogger(this.getClass());
	public PollAndReadStrategy(Connector connector) {
		super(connector);
		
	}

	@Override
	public void doReadAndTransform() throws ConnectorException {
		String strategyName = "PollAndReadStrategy";
		while(true){
			log.info("In " + strategyName + " :Connector "+connector.getName()+"Status :"+connector.getStatus());
			
			if(shouldStop(strategyName))
			{
				break;
			}
			
			log.info("readAndTransform called");
			connector.doReadAndTransform();
		try {
			
			log.info("waiting in " + strategyName +1000*DataTransporterConstants.DEFAULT_READ_POLL_DELAY_IN_SECONDS);
			Thread.sleep(1000*DataTransporterConstants.DEFAULT_READ_POLL_DELAY_IN_SECONDS);
			log.info("waiting in " + strategyName +1000*DataTransporterConstants.DEFAULT_READ_POLL_DELAY_IN_SECONDS);
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
		
		}

	}

	private boolean shouldStop(String strategyName) {
		boolean shouldStop=false;
		ConnectorStatus status = connector.getStatus();
		if(ConnectorStatus.hasFailed(status)||ConnectorStatus.hasBeenDeActivated(status)||ConnectorStatus.mustStop(status))
		{	
			log.info("In " + strategyName + " :Connector "+connector.getName()+" Status:"+status);
			shouldStop=true;
		}
		return shouldStop;
	}

}