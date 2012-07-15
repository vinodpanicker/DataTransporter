package com.datatools.DataTransporter

import java.text.SimpleDateFormat

class DataTransporter  {
	public static boolean hasRead = false;
	public static boolean hasWritten = false;
	public static String PATTERN = "%d{ABSOLUTE} %-5p [%c{1}] %m%n"
	Logger log=new Logger().getLogger(this.getClass());
	public static def status=""
	
	
	
	//	 Register a shutdown hook, one that cleans up connections.
	/*	static {
	 Runtime.getRuntime().addShutdownHook(new Thread("FCM.shutdownHook") {
	 @Override
	 public void run() {
	 ConnectionManager.stopAllConnections();
	 }
	 });
	 }*/
	
	/*public static runWithOutOptions(){
		final Scheduler scheduler = new Scheduler();
		final ArrayList<Connection> connections=ConnectionManager.loadConnections();
		new Thread(){	
					public void run(){
						log.info("Starting Connection!")
						scheduler.startConnections(connections);
					}
				}.start();
			stop()
	}*/
	public static runWithOutOptions(){
		final ArrayList<Connection> connections=ConnectionManager.loadConnections();
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM HH.mm");
		connections.each{
			LoggerUtils.logFileName = it.getName()+" "+dateFormat.format(calendar.getTime());
			log = LoggerUtils.getLoggerInstance(DataTransporter.class)
			
			ConnectionManager.startConnection(it.getName())
			DataTransporter.listen();
			ConnectionManager.stopConnection (it.getName())
		}
	}
	public static String getStatus(){
		return status
	}
	public static main(args) {
		
		CliBuilder cli= new CliBuilder(usage:'dataTransporter -c "ConnectionName"')
		cli.with{
			cli.h(longOpt:'help','usage information');
			cli.x(argName:'start',longOpt:'start',args:1,optionalArg:true,'Start Schedular')
			cli.s(argName:'stop',longOpt:'stop',args:1,optionalArg:true,'Stop Schedular')
			cli.c(argName:'connectionFile',longOpt:'connection',args:1,required:false,'run Connection')
			cli.l(argName:'connectionFileLocation',longOpt:'configfileLocation',args:1,required:false,'configuration file location')
			cli.p(argName:'log4jPropertiesFileLocation',longOpt:'log4jPropertiesLocation',args:1,required:false,'log4j Properties file location')
		}
		DataTransporterConstants.LOG4JFILE_LOCATION = System.getenv("WFC_HOME")
		Logger log = new Logger(LoggerUtils.getLoggerInstance(DataTransporter.class))
		
		if( args.length == 0) {
			runWithOutOptions()
		}
		else{	
			OptionAccessor options=cli.parse(args)
			if(options.h ){
				cli.usage()
			}
			if (options.l){
				DataTransporterConstants.CONFIGFILE_LOCATION = options.l
			}
			if(options.p){
				DataTransporterConstants.LOG4JFILE_LOCATION = options.p
				log = new Logger(LoggerUtils.getLoggerInstance(DataTransporter.class))
			}
			if(options.c){
				def argVlaue = options.c
				activateConnections(argVlaue)
			}
			else{
				log.info ("Running Connections in "+DataTransporterConstants.CONFIGFILE_LOCATION)
				runWithOutOptions()
			}
		}
	}
	
	public static void activateConnections(def connections){
		
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM HH.mm");
		
		ArrayList connectionNames =[]
		String connectionFileName
		File connectionFile
		StringTokenizer st = new StringTokenizer(connections,",")
		
		Logger log = new Logger(LoggerUtils.getLoggerInstance(DataTransporter.class))
		
		while( st.hasMoreTokens()){
			def token = st.nextToken()
			connectionFileName =DataTransporterConstants.CONFIGFILE_LOCATION+token+'.con'; 
			
			connectionFile = new File(connectionFileName);
			
			if(connectionFile.exists()){
				connectionNames.add(token)
			}
			else {
				log.error ("File " +token+" Not Found")
			}
		}
		final ArrayList<Connection> connectionsList=ConnectionManager.loadConnections(connectionNames)
		
		connectionsList.each{
			LoggerUtils.logFileName = it.getName()+" "+dateFormat.format(calendar.getTime());
			log = new Logger(LoggerUtils.getLoggerInstance(DataTransporter.class))
			
			ConnectionManager.startConnection(it.getName())
			DataTransporter.listen();
			ConnectionManager.stopConnection (it.getName())
		}
		
		//		ConnectionManager.stopAllConnections();
	}
	
	static void  listen(){
		if(Connector.readThread==null)
			return;
		while(Connector.readThread.isAlive() || Connector.writeThread.isAlive())
		{
		}
	}
	
	private static start() {
		final Scheduler scheduler = new Scheduler();
		final ArrayList<Connection> connections=ConnectionManager.loadConnections();
		new Thread(){	
					public void run(){
						scheduler.startConnections(connections);
					}
				}.start()
	}
	
	private static stop() {
		final Scheduler scheduler = new Scheduler();
		new Thread(){	
					public void run(){
						scheduler.stopAll();
					}
				}.start()
	}
}