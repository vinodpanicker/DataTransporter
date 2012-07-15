package com.datatools.DataTransporter;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.RollingFileAppender;

public class LoggerUtils {
	static Calendar calendar = Calendar.getInstance();
	static SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM HH.mm");
	public static String logFileName = "dataTransporter "+dateFormat.format(calendar.getTime());
	/*static {
		
		Logger rootLogger = Logger.getRootLogger(); 
		if (!rootLogger.getAllAppenders().hasMoreElements()) {   
			rootLogger.setLevel(Level.INFO);
			try {
				rootLogger.addAppender(new DailyRollingFileAppender(new PatternLayout(PatternLayout.DEFAULT_CONVERSION_PATTERN), "urlogfileName.log", "'.'yyyy-MM-dd"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Logger pkgLogger = rootLogger.getLoggerRepository().getLogger("com.datatools.DataTransporter");  
			
			pkgLogger.setLevel(Level.DEBUG);  
			
			pkgLogger.addAppender(new ConsoleAppender( new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
			pkgLogger.info("hello");
			}
		}
		
		
		DailyRollingFileAppender appender;
		try {
			appender = new DailyRollingFileAppender(new PatternLayout("%-4r [%t] %-5p %c %x - %m%n %d{dd MMM yyyy HH:mm:ss,SSS}"), "urlogfileName.log", "'.'yyyy-MM-dd");
			
			appender.setThreshold(Level.DEBUG);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//    	PropertyConfigurator.configure(DataTransporterConstants.LOG4JFILE_LOCATION+"/"+"log4j.properties");
	}
	*/

	public String getLogFileName() {
		return logFileName;
	}

	public void setLogFileName(String logFileName) {
		this.logFileName = logFileName;
	}

	public static Logger getLoggerInstance(Class className) { 
		
		Logger rootLogger = Logger.getLogger(className);
		rootLogger.removeAllAppenders();
	    rootLogger.setLevel(Level.DEBUG);
	    PatternLayout layout = new PatternLayout(PatternLayout.DEFAULT_CONVERSION_PATTERN);
	    rootLogger.addAppender(new ConsoleAppender(layout));
	    rootLogger.setAdditivity(false);
	    try {
	        RollingFileAppender rfa = new RollingFileAppender(layout, LoggerUtils.logFileName+".log");
	        rfa.setMaximumFileSize(1000000);
	        rootLogger.addAppender(rfa);
	        rootLogger.setAdditivity(false);

	    } catch (IOException e) {
	           //  e.printStackTrace();
	  }
		return rootLogger;
	}
}