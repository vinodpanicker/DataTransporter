package com.datatools.DataTransporter

/**
 * Configuration Class for ScheduleConfiguration in a Connection
 * @author Vinod Panicker
 *
 */
public class ScheduleCfg {
	def workDays; 
	def officeHours
	def startEveryXMinutes
	def startEveryXHours
	static Logger log = DataTransporter.log;
	
	public ScheduleCfg(String workDays, String officeHours,
	String startEveryXMinutes, String startEveryXHours) {
		
		workDays=Eval.me(workDays)
		officeHours= Eval.me(officeHours)
		startEveryXMinutes=startEveryXMinutes
		startEveryXHours=startEveryXHours
		
	}
	
	public ScheduleCfg(Map args) {
		["workDays","officeHours","startEveryXMinutes","startEveryXHours"].each{
			def value=args.get(it)
			if(value==null) {return
			}
			else {
				this."${it}"=Eval.me(value)
			}
		}
		
	}
	
	public String print()
	{
//		return workDays+officeHours+startEveryXMinutes+startEveryXHours
		log.debug (workDays+officeHours+startEveryXMinutes+startEveryXHours)
	}
	
}