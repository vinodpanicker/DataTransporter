package com.datatools.DataTransporter

/***
 * The basic scheduler class for DataTransporter
 * 
 * A connection can be started only by a scheduler or by a call made on a connection got from a ConnectionManager
 * ConnectionManager does not start any connection
 * 
 * @author Vinod Panicker
 *
 */
public class Scheduler {
	
//	Logger log=Logger.getLogger(Scheduler.class)
	 Logger log = DataTransporter.log;
	SchedulerStatus status=SchedulerStatus.NOT_STARTED
	def activeConnections=[]
	
	
	public boolean startConnections(ArrayList<Connection> connections) {
		
		status=SchedulerStatus.STARTING
		
		startConnectionsWithoutSchedule(connections);
		
		while(true) {
			//Stop is trigger for the schedular
			if(status==SchedulerStatus.STOP)
			{
				// will set the schedularStatus to STOPPED_ALL or FAILED__TO_STOPALL
				stopAll()
				return true 
			}
			//externally schedular status is set to PAUSE 
			//so will wait till the status is changed again 
			if(status==SchedulerStatus.PAUSE)
			{
				sleep DataTransporterConstants.SCHEDULER_DEFAULT_DELAY*1000
				return true 
			}
			
			//passed all status check , now activate all connections
			connections.each{
				def connection=it
				if(doesNotAllowMultipleConnection()) {
					return
				}
				if(ConnectionStatus.isActive(it.name)) {
					return
				}
				def schedules=it.getSchedules()
				
				if(schedules==null||schedules.isEmpty()) {
					return
				}
				schedules.each {schedule->
					if(shouldBeStarted(schedule)) {
						activateConnection(connection)
						sleep 10000;
						//						Thread.wait ((schedule.getStartEveryXMinutes()+schedule.getStartEveryXHours()*60)*60000)
					}		
				}
			}
			status=SchedulerStatus.STARTED_ALL
			sleep DataTransporterConstants.SCHEDULER_DEFAULT_DELAY
		}
		return true;
	}
	
	/**
	 * This to ensure only one connection is run at a time
	 * @return
	 */
	public boolean doesNotAllowMultipleConnection() {
		if("yes".equals(DataTransporterConstants.ALLOW_MULTIPLE_CONNECTIONS)) {
			return true
		}
		else {
			return false
		}	
	}
	
	private activateConnection(connection) {
//		Priority priority = new Priority()
		log.info "$connection.name Starting!"
		Thread.start { 
			connection.activate()
			activeConnections<<connection
			log.info ( "connection:"+connection.getName()+" Status:"+connection.getStatus());
		}
	}
	
	/**
	 * only connections that are to be scheduled are left
	 * @param logger
	 * @param connections
	 */
	private void startConnectionsWithoutSchedule(ArrayList<Connection> connections) {
		
		def connectionsToBeActivatedOnce=[]
		connections.each{
			def schedules=it.getSchedules()
			
			if(schedules==null||schedules.isEmpty()) {
				activateConnection(it) 
				connectionsToBeActivatedOnce<<it
				return
			}
		}
		connections=connections-connectionsToBeActivatedOnce
	}
	
	/**
	 * This method will check if the connection should be triggered now
	 * @param schedule
	 * @return
	 */
	private boolean shouldBeStarted(Schedule schedule) {
		boolean shouldStart=false
		def workDays=schedule.getWorkDays()
		def officeHours=schedule.getOfficeHours()
		def everyXminutes=schedule.getStartEveryXMinutes()
		def everyXhours=schedule.getStartEveryXHours()
		
		everyXminutes=everyXminutes+everyXhours*60;
		
		def now = new Date()
		if(workDays.contains(now.day) && officeHours.contains(now.hours) && 0== now.minutes % everyXminutes )
			{
				shouldStart=true
				
			}
		
		return shouldStart
	}
	
	/**
	 * Terminates all connection including connections not started by this connector
	 */
	public void stopAll() {
		Connection[] allActiveConnections =ConnectionManager.listActiveConnections()
		stopActiveConnections(allActiveConnections)
	}
	
	/**
	 * Terminates only connections started by this scheduler
	 */
	public void stopConnectionsActivatedByScheduler() {
		
		stopActiveConnections(activeConnections)
		
	}
	
	private stopActiveConnections(def activeConnections) {
		println activeConnections.inspect()
		activeConnections.each{
			
			log.info "$it.name Stopping!"
			it.deactivate()
		}
		status=SchedulerStatus.STOPPED
	}
	
}