package com.datatools.DataTransporter;

import java.util.ArrayList;
import java.util.Iterator;


/**
 * 
 * @author Vinod Panicker
 * @version 1.0
 * 
 *          This Basic Connection Class.
 * 
 *          Activation Path
 * 
 * <PRE>State 1)NOT_ACTIVATED
 * 
 * <PRE>State 2)CONFIGURATION_LOADED
 * 
 * <PRE>State 3)CONNECTORS_INITIALIZED
 * 
 * <PRE>State 4)RECOVERED_PREVIOUS_RUN_DATA
 * 
 * <PRE>State 5)CONNECTORS_ACTIVATED
 * 
 * <PRE>State 6)CONNECTION_ACTIVATED
 * 
 * Deactivation Path , trigger with a call to deactivateConnection()
 * 
 * <PRE>State 7)DEACTIVATED_CONNECTORS
 * 
 * <PRE>State 8)SAVED_CONNECTION_DATA
 * 
 * <PRE>State 9)CONNECTION_SHUTDOWN
 * 
 * <PRE>State 10)CONNECTION_DEACTIVATED
 * 
 * External Signals states checked in the connectors are
 * 
 * <PRE>CONNECTION_STOP
 * 
 * <PRE>CONNECTION_PAUSE
 * 
 * <PRE>CONNECTION_START
 * 
 * 
 */
public class Connection {

	private static ConnectionContext context = new ConnectionContext();
	private String name;
	private ConnectionStatus status;
	public static String connectionStatus;
	public static String connectionName;
	private ArrayList<Connector> connectors = new ArrayList<Connector>();
	private ArrayList<FieldMapping> fieldMappings = new ArrayList<FieldMapping>();
	// Schedules are used by schedular to kick start/trigger connections
	private ArrayList<Schedule> schedules = null;

	public Data data = initializeDataStore(DataTransporterConstants.IN_MEMORY_DATASTORE);
	Logger log=new Logger().getLogger(this.getClass());
	
	private Data initializeDataStore(String dataStoreType) {
		return new DataQueue(this.name);
//		return new DataQueueForMap(this.name);
	}

	private Connection() {
	}

	/**
	 * 
	 * @param connectionName
	 * @param input
	 */
	public Connection(String connectionName, Object input) {

		this(connectionName);
		// Raw data name can be change , only called Raw at the moment to
		// indicate it is not parsed
		data.setRawData(input);
	}

	public Connection(String connectionName) {
		setName(connectionName);

		setStatus(ConnectionStatus.CONNECTION_NOT_ACTIVATED);
	}

	/**
	 * The activate method called by the Scheduler
	 */
	public void activate() {
		doLoadConnectionConfiguration();
		doInitializeConnectors();
		doRecoverOfAnySavedConnectionData();
		doActivateConnectors();
		monitorConnectors();
	}

	/**
	 * Delegate method that checks the status of the connection before
	 * activateConnector is called
	 */
	private void doActivateConnectors() {
		if (status == ConnectionStatus.CONNECTORS_INITIALIZED
				|| status == ConnectionStatus.RECOVERED_PREVIOUS_RUN_DATA)
			activateConnectors();
		if (status.equals(ConnectionStatus.CONNECTORS_ACTIVATED)) {
			setStatus(ConnectionStatus.CONNECTION_ACTIVATED);
			log.debug("Connection:" + this.name + ":" + status);
		} else {
			log.debug("Connection:" + this.name + ":" + status);
		}

	}

	/**
	 * Delegate method that would be used to check connection status before
	 * doing any recovery , the last run should have logged the connector status
	 * in the connectors last run log and fetch data from the Internal
	 * Connection recovery dataStore and put it on the Internal or InMemory
	 * DataStor
	 */
	private void doRecoverOfAnySavedConnectionData() {
		if (status == ConnectionStatus.RECOVER_LAST_RUN_DATA) {
			setStatus(ConnectionRecoveryAgent.recover(this.name, this.data));
			recoverAnySavedConnectionData();
		}

	}

	/**
	 * Delegate method used to check the connector status before initializing
	 * the Connector
	 */
	private void doInitializeConnectors() {
		if (status == ConnectionStatus.CONFIGURATION_LOADED) {
			initializeConnectors();
		}

	}

	private void doLoadConnectionConfiguration() {
		if (status == ConnectionStatus.CONNECTION_NOT_ACTIVATED) {
			loadConnectionConfiguration();

		}
	}

	/**
	 * Method to recover any data that has been saved in the previous run of the
	 * connection The ConnectionRecoveryAgentMethod recovers at the connection
	 * level this method is called post that. In each connector the mechanism to
	 * recovery from last run failure/incomplete last run may be added
	 * independently (To be explored further.
	 * 
	 * 
	 */
	protected void recoverAnySavedConnectionData() {
		// TODO need to check if ConnectionRecoveryAgent did its job
		setStatus(ConnectionStatus.RECOVERED_PREVIOUS_RUN_DATA);
	}

	public void deactivate() {
		doDeactivateConnectors();
		doSaveConnectionData();
		doShutdownConnection();
		if (status.equals(ConnectionStatus.CONNECTION_SHUTDOWN)) {
			// POC
			setStatus(ConnectionStatus.CONNECTION_DEACTIVATED);
			// setStatus(ConnectionStatus.CONNECTION_NOT_ACTIVATED);
		}
	}

	/**
	 * Delegate method to validate connection status before shutdown
	 */
	private void doShutdownConnection() {
		shutdownConnection();

	}

	/**
	 * Move what is left in the Internal/ In-memory dataStore pushed to the
	 * connection recovery DataStore
	 */

	private void doSaveConnectionData() {
		if (status == ConnectionStatus.DEACTIVATED_CONNECTORS)
			saveConnectionData();

	}

	/**
	 * Deactivate will be called without doing any check on the connection
	 * status. A guard condition can be added which will not call a deactivate
	 * on an already deactivated Connection
	 */

	private void doDeactivateConnectors() {
		if (status == ConnectionStatus.CONNECTION_SHUTDOWN)
			return;

		deactivateConnectors();

	}

	protected void shutdownConnection() {
		setStatus(ConnectionStatus.CONNECTION_SHUTDOWN);

	}

	/**
	 * This method saves any connection related data into a datastore if
	 * present.
	 */
	protected void saveConnectionData() {
		if (data.count() == 0) {
			setStatus(ConnectionStatus.SAVED_CONNECTION_DATA);
		} else {
			setStatus(ConnectionRecoveryAgent.save(this.name, data));
		}
	}

	protected void deactivateConnectors() {
		// Mode Pull connectors to be deactivated before Mode Push Connectors.
		// Write channels to be opened before read happens.
		deactivateConnectors(Connector.MODE_PULL);
		deactivateConnectors(Connector.MODE_PUSH);
		setStatus(ConnectionStatus.DEACTIVATED_CONNECTORS);

	}

	/**
	 * Method to activate connectors and monitor Connectors
	 */
	protected void activateConnectors() {

		// Mode Push connectors to be activated before Mode Pull Connectors.
		// Write channels to be opened before read happens.
		activateConnectors(Connector.MODE_PUSH);
		activateConnectors(Connector.MODE_PULL);
		setStatus(ConnectionStatus.CONNECTORS_ACTIVATED);

	}

	/**
	 * Method check on the connection Manager if any notification has been
	 * received back from the connecters notifying failure or have finished
	 * their read/write operations
	 */
	private void monitorConnectors() {
		
		while (getStatus().equals(ConnectionStatus.CONNECTION_DEACTIVATED)) {
			ArrayList<Connector> modeconnectors;
			modeconnectors = getConnectors(Connector.MODE_PUSH);
			for (Connector modeConnector : modeconnectors) {

				ConnectorStatus connectorStatus = modeConnector.getStatus();
				log.debug("Checking Connector" + modeConnector.getName()
						+ " Status :" + connectorStatus);
				if (ConnectorStatus.hasFailed(connectorStatus)) {
					log.error("PUSH Connector" + modeConnector.getName()
							+ " Has Failed!! ");
					setStatus(ConnectionStatus.CONNECTOR_FAILED);
					pauseConnectors();
					saveConnectionData();
					log.debug("Connection saved " + modeConnector.getName()
							+ " Connection Status :" + getStatus());
				}
			}

			modeconnectors = getConnectors(Connector.MODE_PULL);
			for (Connector modeConnector : modeconnectors) {

				ConnectorStatus connectorStatus = modeConnector.getStatus();
				log.debug("Checking Connector" + modeConnector.getName()
						+ " Status :" + connectorStatus);
				if (ConnectorStatus.hasFailed(connectorStatus)) {
					log.error("PULL Connector" + modeConnector.getName()
							+ " Has Failed!! ");
					setStatus(ConnectionStatus.CONNECTOR_FAILED);
				}
			}
		}
	}

	/**
	 * This method send PAUSE Signal to Active connectors returns when the
	 * Connector responds with PAUSED
	 */
	private void pauseConnectors() {
		for (Connector connector : connectors) {
			if (ConnectorStatus.hasFailed(connector.getStatus())) {
				break;
			}
			connector.setStatus(ConnectorStatus.PAUSE);
			while (ConnectorStatus.hasPaused(connector.getStatus())) {
				log.debug("Pausing Connector" + connector.getName()
						+ " Status :" + connector.getStatus());
			}
		}

	}

	private void activateConnectors(String mode) {
		ArrayList<Connector> modeconnectors;
		modeconnectors = getConnectors(mode);
		for (Connector modeConnector : modeconnectors) {

			/*log.info("Activating Connector" + modeConnector.getName()
					+ " Status :" + modeConnector.getStatus());*/
			log.info( modeConnector.getName()
					+ ":" + modeConnector.getStatus());
			if (hasConnectorActivationFailed()) {
				break;
			}
			modeConnector.activate();

			// read/write Thread starts in the init of the connector

			while (connectorIsBeingActivated(modeConnector.getStatus())) {

				try {
					wait();
				} catch (InterruptedException e) {

					e.printStackTrace();
				}
				// wait till connector is STARTED-------
				// ConnectorStatus.ACTIVATED
				// if connection gets ConnectorStatus as FAILED
				// break and deactivate connection.

				// TODO need to check if STARTED is used.
				ConnectorStatus connectorStatus = modeConnector.getStatus();
				// todo introduce logger
				log.info("Waiting for Connector" + modeConnector.getName()
						+ " Status :" + connectorStatus);

				if (ConnectorStatus.hasFailed(connectorStatus)) {
					setStatus(ConnectionStatus.CONNECTOR_ACTIVATION_FAILED);
					break;
				}

				if (ConnectorStatus.hasBeenActivated(connectorStatus)) {
					setStatus(ConnectionStatus.CONNECTOR_ACTIVATED);
					break;
				}
			}

		}

	}

	private boolean hasConnectorActivationFailed() {

		return getStatus() == ConnectionStatus.CONNECTOR_ACTIVATION_FAILED;
	}

	private boolean connectorIsBeingActivated(ConnectorStatus connectorStatus) {
		// Todo replace with log
      log.debug("Checking if connector: " + this.name + "is being activated");
		return ConnectorStatus.isBeingActivated(connectorStatus);
	}

	private void deactivateConnectors(String mode) {
		ArrayList<Connector> modeconnectors;
		modeconnectors = getConnectors(mode);

		for (Connector modeConnector : modeconnectors) {


			log.info(modeConnector.getName()
					+ ":" + modeConnector.getStatus());
			// TODO replace the logic below with wait like in case of activate
			// if (modeConnector.getStatus().equals(ConnectorStatus.STOPPED)) {
			// continue;
			// } else {
			// setStatus(ConnectionStatus.CONNECTOR_DEACTIVATION_FAILED);
			// }

			// /////////////
			if (hasConnectorDeActivationFailed()) {
				break;
			}

			modeConnector.deactivate();

			// read/write Thread being stopped in the connector

			while (connectorIsBeingDeActivated(modeConnector.getStatus())) {
				try {
					wait();
				} catch (InterruptedException e) {

					e.printStackTrace();
				}
				// wait till connector is STOPPED-------
				// ConnectorStatus.DEACTIVATED
				// if connection gets ConnectorStatus as FAILED
				// break and set connection deactivation failed.

				// TODO need to check if STOPPED is used.
				ConnectorStatus connectorStatus = modeConnector.getStatus();
				// todo introduce logger
				log.info("Waiting for Connector" + modeConnector.getName()
						+ " Status :" + connectorStatus);

				if (ConnectorStatus.hasFailed(connectorStatus)) {
					setStatus(ConnectionStatus.CONNECTOR_DEACTIVATION_FAILED);
					break;
				}

				if (ConnectorStatus.hasBeenDeActivated(connectorStatus)) {
					setStatus(ConnectionStatus.CONNECTOR_DEACTIVATED);
					log.debug("connector: " + this.name
							+ "has been deactivated");
					
					break;
				}
			}
		}
	}

	private boolean hasConnectorDeActivationFailed() {

		return getStatus() == ConnectionStatus.CONNECTOR_DEACTIVATION_FAILED;
	}

	private boolean connectorIsBeingDeActivated(ConnectorStatus connectorStatus) {
		// Todo replace with log
		log.debug("Checking if connector: " + this.name
				+ "is being deactivated");
		return ConnectorStatus.isBeingDeActivated(connectorStatus);

	}

	protected ArrayList<Connector> getConnectors(String mode) {

		ArrayList<Connector> connectorlist = new ArrayList<Connector>();
		for (Connector connector : connectors) {
			if (mode.equals(connector.getMode())) {
				connectorlist.add(connector);
			}
		}

		return connectorlist;
	}

	/**
	 * Method that initializes Connectors
	 */
	protected void initializeConnectors() {
		// load connectors from the Connector Configuration load in the context
		// and initialize them
		connectors.clear();
		String[] connectorConfigurations = context.getConnectorConfiguration();
		for (String connectorConfiguration : connectorConfigurations) {
			Connector connector = ConnectorFactory.getConnector(
					connectorConfiguration, data, context);
			if (connector == null) {
				status = ConnectionStatus.CONNECTOR_ACTIVATION_FAILED;
				return;
			}
			// associate connector to connection
			connector.setConnectionName(getName());
			connectors.add(connector);
		}
		setStatus(ConnectionStatus.CONNECTORS_INITIALIZED);
	}

	/**
	 * Method that loads the Configuration
	 */
	protected void loadConnectionConfiguration() {
		// Load all parameter for the connection and associated connectors
		ConfigLoader configLoader = getConfigLoader();
		configLoader.loadConnectionConfiguration(this);
		setStatus(ConnectionStatus.CONFIGURATION_LOADED);
	}

	protected ConfigLoader getConfigLoader() {
		return new DefaultConfigLoader();
	}

	public void setFieldMappings(ArrayList<FieldMapping> fieldMappings) {
		this.fieldMappings = fieldMappings;
	}

	public void setConnectors(ArrayList<Connector> connectors) {
		this.connectors = connectors;
	}

	protected ArrayList<Connector> getConnectors() {

		return connectors;
	}

	protected ArrayList<FieldMapping> getFieldMappings() {

		return fieldMappings;
	}

	public void setName(String name) {
		this.name = name;
		connectionName = name;
	}

	public String getName() {

		return name;
	}

	public synchronized void setStatus(ConnectionStatus status) {
		this.status = status;
		connectionStatus = status.toString();

		notifyAll();

	}

	public ConnectionStatus getStatus() {
		return status;
	}

	public static ConnectionContext getContext() {

		return context;
	}

	public void setContext(ConnectionContext context) {
		this.context = context;

	}

	public Data getConnectionData() {

		return data;
	}

	/**
	 * Schedule gets loaded before the connection is activated. Need to check if
	 * the schedularConfiguration is available before the scheduler activates
	 * the connection
	 * 
	 * @return
	 */
	public ArrayList<Schedule> getSchedules() {

		if (schedules == null) {
			ConfigLoader configLoader = getConfigLoader();
			configLoader.loadScheduleConfiguration(this);

		}
		return schedules;

	}

	public void setSchedules(ArrayList<Schedule> schedules) {
		this.schedules = schedules;
	}
}
