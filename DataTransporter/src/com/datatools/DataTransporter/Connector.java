package com.datatools.DataTransporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * 
 * @author Vinod Panicker
 * @version 1.0
 * 
 *          All connectors should extend Connector class
 * 
 * 
 *          The Happy Path for Connector class
 * 
 * <pre>State 1)NOT_ACTIVATED ,Fail status: CONNECTOR_NOT_FOUND </pre>
 * 
 * <pre>State 2)CONNECTED , Fail status: FAILED_TO_CONNECT </pre>
 * 
 * <pre>State 3)PRE_INIT_VALIDATION_DONE , Fail status:PRE_INIT_VALIDATION_FAILED </pre>
 * 
 * <pre> State 4)ACTIVATED ,Fail status :ACTIVATION_FAILED </pre>
 * 
 * <pre>State 5)READ_COMPLETED , In Process Status: READING,Fail status:READ_FAILED </pre>
 * 
 * <pre>State 6)WRITE_COMPLETED, In Process Status: WRITING,Fail status:WRITE_FAILED </pre>
 * 
 * <pre>State 7)CLEANUP_DONE, Fail status:CLEANUP_FAILED </pre>
 * 
 * <pre>State 8)DISCONNECTED,Fail status:CANNOT_DISCONNECT</pre>
 * 
 * <pre>State 9)DEACTIVATED,Fail status: DEACTIVATION_FAILED</pre>
 * 
 * 
 *          Signal status are
 * 
 *<pre>STOP  ,Connector Response Status :STOPPED || FAILED_TO_STOP</pre>
 * 
 *<pre>START ,Connector Response Status :STARTED || FAILED_TO_START</pre>
 * 
 *<pre> PAUSE ,Connector Response Status :PAUSED  || FAILED_TO_PAUSE</pre>
 * 
 */

public abstract class Connector implements ConnectorLifeCycle {
	Logger log=new Logger().getLogger(this.getClass());

	public static final String MODE_PULL = "PULL";
	public static final String MODE_PUSH = "PUSH";
	
	protected ConnectorStatus status;
	public static String pullStatus;
	public static String pushStatus;
	public static String pushName;
	public static String pullName;
	public static boolean readStatus=false;
	private String mode;
	private String name;
	private String type;
	// used to look up the status of the associated connection
	private String connectionName;

	// Additional Parameters configured for a connector
	private HashMap<String, String> parameters;

	// dataStore associated with the connector
	private Data data;

	// fields data structure associated with a connector
	private HashMap<String, String> fields;

	private List<ConnectorFieldMapping> connectorFieldMappings;
	public static Thread readThread;
	public static Thread writeThread;

	// transformer used to transform the fields of a record
	private Transformer transformer = getTransformer();
	private FilterMesh filterMesh = getFilterMesh();
	// Strategy Type used for read or write
	private StrategyType readStrategyType = StrategyType.ONE_TIME_READ;
	private StrategyType writeStrategyType = StrategyType.POLL_AND_WRITE;

	public Connector(String name) {
		setName(name);
		setStatus(ConnectorStatus.NOT_ACTIVATED);
		ConfigLoader configLoader = getConfigLoader();
		configLoader.loadConnectorConfiguration(this);
	}

	// this method will activate connectors based on connection configuration

	public void activate() {
		doConnect();
		doPreInitValidation();
		doInit();
	}

	/**
	 * This method will also validate the connector status and also manage
	 * recovery if the connector bails out in the connect phase during the
	 * previous run
	 */

	private void doConnect() {
		try {
			// No status check done here.
			connect();
		} catch (Exception e) {
			// TODO Log exception
			setStatus(ConnectorStatus.FAILED_TO_CONNECT);
		}
		if (ConnectorStatus.hasFailed(status)) {
			notifyActivationFailureToConnection();
			return;
		}
		setStatus(ConnectorStatus.CONNECTED);
	}

	/**
	 * This method will also validate the connector status and also manage
	 * recovery if the connector bails out in the prevalidation phase during the
	 * previous run
	 */

	private void doPreInitValidation() {
		if (status == ConnectorStatus.CONNECTED) {
			try {
				preInitValidation();
			} catch (Exception e) {

				setStatus(ConnectorStatus.PRE_INIT_VALIDATION_FAILED);
			}
		} else {
			setStatus(ConnectorStatus.FAILED_TO_CONNECT);
		}

		// ConnectorStatus must be ConnectorStatus.PRE_INIT_VALIDATION_DONE
		if (ConnectorStatus.hasFailed(status)) {
			notifyActivationFailureToConnection();
			return;
		}
		setStatus(ConnectorStatus.PRE_INIT_VALIDATION_DONE);
	}

	/**
	 * This method will also validate the connector status and also manage
	 * recovery if the connector bails out in the init phase during the previous
	 * run
	 */
	private void doInit() {
		if (status == ConnectorStatus.PRE_INIT_VALIDATION_DONE) {
			// in Connector Init if it fails status must be set as
			// CONNECTOR_ACTIVATION_FAILED.
			init();
		} else {
			setStatus(ConnectorStatus.PRE_INIT_VALIDATION_FAILED);
		}

		if (ConnectorStatus.hasFailed(status)) {
			notifyActivationFailureToConnection();
			return;
		}

		setStatus(ConnectorStatus.ACTIVATED);
	}

	private void notifyActivationFailureToConnection() {
		ConnectionManager.setConnectionStatus(this.connectionName,
				ConnectionStatus.CONNECTOR_ACTIVATION_FAILED);
	}

	/**
	 * Method used to pause the connector few connectors can be controlled with
	 * pause
	 */
	public void pause() {
		setStatus(ConnectorStatus.PAUSE);
	}


	public void deactivate() {

		doStop();
		doCleanup();
		doDisconnect();
		if (ConnectorStatus.hasFailed(status)) {

			return;
		}
		setStatus(ConnectorStatus.DEACTIVATED);
	}

	private void doDisconnect() {
		if (status == ConnectorStatus.CLEANUP_DONE) {
			disconnect();
		}
		if (ConnectorStatus.hasFailed(status)) {
			return;
		}

		setStatus(ConnectorStatus.DISCONNECTED);
	}

	private void doCleanup() {
		if (status == ConnectorStatus.STOPPED) {
			cleanup();
		}
		if (ConnectorStatus.hasFailed(status)) {
			return;
		}

		setStatus(ConnectorStatus.CLEANUP_DONE);
	}

	private void doStop() {
		// send signal to write/read thread
		setStatus(ConnectorStatus.STOP);
		stop();

		if (ConnectorStatus.hasFailed(status)) {
			return;
		}

		setStatus(ConnectorStatus.STOPPED);
	}

	/**
	 * Method that must be implemented by respective connector to clean up any
	 * metadata/data temporary in nature for required for the connector
	 */
	public abstract void cleanup();

	/**
	 * Method to stop the connector
	 */
	public abstract void stop();

	// this method starts the data transfer for the connection
	protected void init() {
		// Need to return if status is not connected. .. check for status
		// connected missing.
		process(this.mode);
	}

	private void process(final String mode) {
		if(mode == Connector.MODE_PULL)
		{
		readThread = new Thread() {

			public void run() {
				try {
						getReadStrategy().doReadAndTransform();
				} catch (ConnectorException e) {
					log.error("Connector Error " + e.getMessage());
					
						if (ConnectorStatus.hasFailed(status)) {
							return;
						} else {
							
								if (getStatus() == ConnectorStatus.READ_COMPLETED) {
									Thread.currentThread().interrupt();
							}
						}
				}
			}
			};
			readThread.start();
		}
		else{
			writeThread = new Thread() {

				public void run() {
					try {
						getWriteStrategy().doFilterAndWrite();
					} catch (ConnectorException e) {
						log.error("Connector Error " + e.getMessage());
						
							if (ConnectorStatus.hasFailed(status)) {
								return;
							} else {
									if (getStatus() == ConnectorStatus.WRITE_COMPLETED) {
										Thread.currentThread().interrupt();
									}
							}
					}
				}
				};
				writeThread.start();
		}
	}
	private ConnectorReadStrategy getReadStrategy() {
		ConnectorReadStrategy connectorReadStrategy = null;

		switch (readStrategyType) {

		case ONE_TIME_READ:
			connectorReadStrategy = new OneTimeReadStrategy(this);
            log.info("read Strategy:ONE_TIME_READ Triggered");
			break;

		case POLL_AND_READ:
			connectorReadStrategy = new PollAndReadStrategy(this);
			break;

		default:
			connectorReadStrategy = new OneTimeReadStrategy(this);

		}
		return connectorReadStrategy;
	}

	private ConnectorWriteStrategy getWriteStrategy() {

		ConnectorWriteStrategy connectorWriteStrategy = null;

		switch (writeStrategyType) {

		case ONE_TIME_WRITE:
			connectorWriteStrategy = new OneTimeWriteStrategy(this);
           log.info("write Strategy:ONE_TIME_WRITE Triggered");
			break;

		case POLL_AND_WRITE:
			connectorWriteStrategy = new PollAndWriteStrategy(this);
			PollAndWriteStrategy.data=this.data;
            log.info("write Strategy:POLL_AND_WRITE Triggered");
			break;

		default:
			connectorWriteStrategy = new OneTimeWriteStrategy(this);

		}
		return connectorWriteStrategy;
	}

	protected void doReadAndTransform() throws ConnectorException {
       log.info("readAndTransform Called");
		setStatus(ConnectorStatus.READING);
		readAndTransform();
		if (ConnectorStatus.hasFailed(status)) {
			return;
		}
		// can include warning if the connector does not set the status as read
		// completed
		setStatus(ConnectorStatus.READ_COMPLETED);
	}

	protected void doFilterAndWrite() throws ConnectorException {
        log.info("filterAndWrite Called");
		setStatus(ConnectorStatus.WRITING);
		filterAndWrite();
		if (ConnectorStatus.hasFailed(status)) {
			return;
		}
		// can include warning if the connector does not set the status as write
		// completed
		setStatus(ConnectorStatus.WRITE_COMPLETED);
	}

	/**
	 * The Filter and Write can be overridden for custom connectors where it is not a symmetric transformation
	 * @throws ConnectorException
	 */
	
	protected void filterAndWrite() throws ConnectorException {

		ArrayList recordsToWrite = readFromConnectionData();
		ArrayList<HashMap> recordsReadyForFilter = switchToConnectorFieldNames(recordsToWrite);

		filterAndWrite(recordsReadyForFilter);

	}

	/**
	 * The Filter and Write can be overridden for custom connectors where it is not a symmetric transformation
	 * @throws ConnectorException
	 */
	protected void filterAndWrite(ArrayList<HashMap> recordsReadyForFilter)
			throws ConnectorException {
		
		// InvokeTransformation on Values where there is a match in field
		// mapping names

		// To be added later using a special flag where transformation can also
		// be performed before a write
		// ArrayList<HashMap> recordsTransformed =
		// performTransformations(recordsToWrite);

		// Filter out record not required to be written
		ArrayList<HashMap> recordsFiltered = filterRecords(recordsReadyForFilter);

		write(recordsFiltered);
	}

	private ArrayList<HashMap> switchToConnectorFieldNames(
			ArrayList<HashMap> inRecords) {
		ArrayList<HashMap> outRecords = new ArrayList<HashMap>();
		if (inRecords == null) {
			return outRecords;
		}

		for (Iterator iterator = inRecords.iterator(); iterator.hasNext();) {

			if (ConnectionStatus.isPausedOrFailed(connectionName)) {
				setStatus(ConnectorStatus.PAUSE);
				return outRecords;
			}

			if (ConnectionStatus.isStopped(connectionName)) {
				setStatus(ConnectorStatus.STOPPING);
				deactivate();
				return outRecords;
			}

			HashMap<String, Object> inrecord = (HashMap<String, Object>) iterator
					.next();
			HashMap outrecord = swapFieldNamesInRecord(inrecord);
			outRecords.add(outrecord);
		}
		return outRecords;
	}

	private ArrayList readFromConnectionData() {

		if (isConnectionPausedOrFailed()) {
			setStatus(ConnectorStatus.PAUSE);
			return null;
		}

		if (isConnectionStopped()) {
			setStatus(ConnectorStatus.STOPPING);
			deactivate();
			return null;
		}

		return (ArrayList) data.getRecord();
	}

	/**
	 * The read amd transfrom can be overridden for custom connectors where it is not a symmetric transformation
	 *  (i.e it is a many rows into one column etc )
	 * @throws ConnectorException
	 */
	protected void readAndTransform() throws ConnectorException {

		// The first read which is connector specific read .This read will
		// populate records(individual HashMaps) having FieldMapping names and
		// values
		ArrayList recordsRead = read();

		// Filter out record not required to be written (Filter can be
		// introduced later using a special flag )
		// ArrayList<HashMap> recordsFiltered =
		// filterRecords(recordsTransformed);

		ArrayList<HashMap> recordsReadyForDataStore = switchToFieldMappingNames(recordsRead);

		// InvokeTransformation on Values where there is a match in field
		// mapping names

		ArrayList<HashMap> recordsTransformed = performTransformations(recordsReadyForDataStore);

		// Push the data back on to the datastore/connection data
		writeToConnectionData(recordsTransformed);
		// ps:The write counterpart will read field mapping name and the replace
		// it with target connector field names
	}

	/**
	 * Need to ensure atomicity of operation
	 * 
	 * @param inRecords
	 * @return
	 */
	public ArrayList<HashMap> switchToFieldMappingNames(
			ArrayList<HashMap> inRecords) {
		ArrayList<HashMap> outRecords = new ArrayList<HashMap>();
		if (inRecords == null) {
			return outRecords;
		}

		for (Iterator iterator = inRecords.iterator(); iterator.hasNext();) {

			if (status == ConnectorStatus.PAUSE) {
				setStatus(ConnectorStatus.PAUSED);
				return outRecords;
			}

			if (isConnectionPausedOrFailed()) {
				setStatus(ConnectorStatus.PAUSED);
				return outRecords;
			}
			

			if (isConnectionStopped()) {
				setStatus(ConnectorStatus.STOPPING);
				deactivate();
				return outRecords;
			}

			HashMap<String, Object> inrecord = (HashMap<String, Object>) iterator
					.next();
			HashMap outrecord = swapFieldNamesInRecord(inrecord);
			outRecords.add(outrecord);
		}
		return outRecords;
	}

	private HashMap swapFieldNamesInRecord(HashMap<String, Object> inRecord) {
		HashMap<String, Object> outRecord = new HashMap<String, Object>();
		for (ConnectorFieldMapping connectorfieldMapping : connectorFieldMappings) {
			String fieldMappingName = connectorfieldMapping
					.getConnectionFieldMappingName();
			String field = connectorfieldMapping.getName();
			if (mode == MODE_PULL) {
				outRecord.put(fieldMappingName, inRecord.get(field));
			} else {
				outRecord.put(field, inRecord.get(fieldMappingName));
			}
		}
		return outRecord;
	}

	private ArrayList<HashMap> filterRecords(ArrayList<HashMap> inRecords) {
		ArrayList<HashMap> outRecords = new ArrayList<HashMap>();

		if (inRecords == null || inRecords.isEmpty())
			return outRecords;

		for (Iterator<HashMap> iterator = inRecords.iterator(); iterator
				.hasNext();) {
			HashMap inrecord = iterator.next();

			if (isConnectionPausedOrFailed()) {
				setStatus(ConnectorStatus.PAUSE);
				return outRecords;
			}

			if (isConnectionStopped()) {
				setStatus(ConnectorStatus.STOPPING);
				deactivate();
				return outRecords;
			}

			// TODO all records other than explicit shouldnotbefiltered records
			// or shouldBeFiltered records must be a pass through
			if (shouldNotBeFiltered(inrecord)) {
				outRecords.add(inrecord);
			}
			// else if(shouldBeFiltered(inrecord))
			// {
			// continue;
			// }
			// else
			// {
			// pass through
			// outRecords.add(inrecord);
			// }

		}
		return outRecords;
	}

	/**
	 * Filter out records which are not required
	 * 
	 * @param inrecord
	 * @return
	 */
	private boolean shouldNotBeFiltered(HashMap inRecord) {
		return filterMesh.filter(this.name, connectorFieldMappings, inRecord);

	}

	private void writeToConnectionData(ArrayList<HashMap> recordsTransformed) {
		data.addRecord(recordsTransformed);
	}

	public HashMap<String, String> getFields() {
		return fields;
	}

	public ArrayList<HashMap> performTransformations(ArrayList inRecords) {

		ArrayList<HashMap> outRecords = new ArrayList<HashMap>();
		if (inRecords == null) {
			return outRecords;
		}

		for (Iterator iterator = inRecords.iterator(); iterator.hasNext();) {

			if (isConnectionPausedOrFailed()) {
				setStatus(ConnectorStatus.PAUSE);
				return outRecords;
			}

			if (isConnectionStopped()) {
				setStatus(ConnectorStatus.STOPPING);
				deactivate();
				return outRecords;
			}

			HashMap<String, Object> inrecord = (HashMap<String, Object>) iterator
					.next();
			HashMap outrecord = transform(inrecord);
			outRecords.add(outrecord);
		}
		return outRecords;
	}

	/**
	 * @return
	 */
	protected boolean isConnectionStopped() {
		return ConnectionStatus.isStopped(connectionName);
	}

	/**
	 * @return
	 */
	protected boolean isConnectionPausedOrFailed() {
		return ConnectionStatus.isPausedOrFailed(connectionName);
	}

	/**
	 * Method to transform field in a record using transformation methods
	 * 
	 * @param inrecord
	 * @return
	 */
	private HashMap transform(HashMap<String, Object> inRecord) {
		HashMap outRecord = new HashMap();
		outRecord = transformer.transform(this.name, connectorFieldMappings,
				inRecord);
		return outRecord;
	}



	public abstract void connect();


	public abstract void disconnect();


	public abstract void preInitValidation();


	public abstract ArrayList read() throws ConnectorException;


	public abstract void write(ArrayList recordsToWrite)
			throws ConnectorException;

	/**
	 * gets the Status of the Connector
	 * 
	 * @return
	 */
	public ConnectorStatus getStatus() {
		return status;
	}

	/**
	 * Gets the operating mode of the connector
	 * 
	 * @return
	 */

	public String getMode() {

		return mode;
	}

	public void setName(String name) {
		this.name = name;
		if(this.mode==MODE_PUSH)
			pushName = name;
		else if(pullName==null)
			pullName = name;
	}

	public String getName() {
		return name;
	}

	public synchronized void setStatus(ConnectorStatus status) {
		if(this.mode==MODE_PULL)
			pullStatus=status.toString();
		else
			pushStatus=status.toString();
		if (getStatus() == ConnectorStatus.STOP) {
			//needs to be commented
			log.info("Connector:[" + this.name + " ]in Connection:["
					+ this.connectionName + "] Status :" + getStatus());
			log.info( this.name + " :" + getStatus());
			if (status == ConnectorStatus.STOPPED) {
				checkAndNotifyConnectionFailure(status);
			}
			notifyAll();
			return;
		}

		checkAndNotifyConnectionFailure(status);
	}

	/**
	 * @param status
	 */
	private void checkAndNotifyConnectionFailure(ConnectorStatus status) {
		this.status = status;
		if (ConnectorStatus.hasFailed(status)) {
			log.error(this.name + " Failed!!");
			notifyConnectionFailed(this.connectionName);
		}
		
	}

	/**
	 * Method notifies ConnectionManager about connection Failure
	 * 
	 * @param connectionName
	 */
	private void notifyConnectionFailed(String connectionName) {
		ConnectionManager.setConnectionStatus(this.connectionName,
				ConnectionStatus.CONNECTOR_FAILED);
	}

	public List<ConnectorFieldMapping> getConnectorFieldMappings() {
		return connectorFieldMappings;
	}

	/**
	 * May be further separated to make use of FilterMesh Factories
	 * 
	 * @return
	 */
	private FilterMesh getFilterMesh() {

		return new DefaultFilterMesh();
	}

	/**
	 * May be further separated to make use of TransformerFactories later
	 * 
	 * @return
	 */
	private Transformer getTransformer() {

		return new DefaultTransformer();
	}

	public String getConnectionName() {
		return connectionName;
	}

	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}

	// TODO to be moved to a factory.
	protected ConfigLoader getConfigLoader() {
		return new DefaultConfigLoader();
	}

	public void setConnectorFieldMappings(
			List<ConnectorFieldMapping> connectorFieldMappings) {
		this.connectorFieldMappings = connectorFieldMappings;
	}

	public void setFields(HashMap<String, String> fields) {
		this.fields = fields;
	}

	public HashMap<String, String> getParameters() {
		return parameters;
	}

	public String get(String parameter) {
		return parameters.get(parameter);
	}

	/**
	 * Method to return more than one value for a given key. key would be
	 * postfixed with the number, (1-9) if there are 2 keys, (key1,key2) values
	 * will be returned as a list, when key is passed as parameter.
	 * 
	 * @param key
	 * @return
	 */
	public List getMultipleValues(String key) {
		Iterator<String> iterator = parameters.keySet().iterator();
		ArrayList<String> keyList = new ArrayList<String>();
		while (iterator.hasNext()) {
			String str = iterator.next();
			if (str.startsWith(key) && str.length() <= key.length() + 1) {
				keyList.add(parameters.get(str));
			}
		}

		return keyList;
	}

	public void setParameters(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}

	public Data getData() {
		return data;
	}

	public void setData(Data data) {
		this.data = data;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	
}