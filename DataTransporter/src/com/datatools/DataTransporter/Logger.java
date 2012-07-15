package com.datatools.DataTransporter;

/**
 * Wrapper Logger Class for DataTools
 * @author Vinod Panicker
 *
 */
public class Logger {
	static org.apache.log4j.Logger log=  getLogger();
	
	public Logger(org.apache.log4j.Logger log)
	{
		this.log=log;
	}
	
	public Logger()
	{
		this(getLogger());
	}

	public Logger getLogger(Class classBeingLogged) {
	setLogger(org.apache.log4j.Logger.getLogger(classBeingLogged));
		return this;
	}

	private void setLogger(org.apache.log4j.Logger log) {
		this.log=log;
	}

	private static org.apache.log4j.Logger getLogger() {
		return org.apache.log4j.Logger.getLogger(Logger.class);
	}

	public void error(String msg) {
		log.error(msg);
		
	}

	public void info(String msg) {
		log.info(msg);
		
	}
	
	public void info(Object msg) {
		log.info(msg);
		
	}

	public void debug(String msg) {
		log.debug(msg);
		
	}

}
