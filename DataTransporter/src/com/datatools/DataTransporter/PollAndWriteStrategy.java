package com.datatools.DataTransporter;

public class PollAndWriteStrategy extends AbstractConnectorWriteStrategy
implements ConnectorWriteStrategy {

	Logger log=new Logger().getLogger(this.getClass());
public static Data data ;
public PollAndWriteStrategy(Connector connector) {
super(connector);

}

@Override
public void doFilterAndWrite() throws ConnectorException {
String strategyName = "PollAndWriteStrategy";
while(true){
	ConnectorStatus status = connector.getStatus();
	connector.doFilterAndWrite();
	if((Connector.readStatus)&&(data.count()==0))
	{
		break;
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