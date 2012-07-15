package com.datatools.DataTransporter

public class ScheduleHelper {

	public static ArrayList<Schedule> getSchedulesfromScheduleCfg(ArrayList scheduleCfgs) {
		
		ArrayList<Schedule> schedules=[];
//		for (Iterator iterator = scheduleCfgs.iterator(); iterator.hasNext();) {
//			ScheduleCfg scheduleCfg = (ScheduleCfg) iterator.next();
//			Schedule schedule=new Schedule()
//			schedule = scheduleCfg			
//			schedules<< schedule
//		}
		scheduleCfgs.each {
			Schedule schedule=new Schedule();
			
			schedule.setOfficeHours(it.officeHours)
			schedule.setWorkDays(it.workDays)
			schedule.setStartEveryXHours(it.startEveryXHours)
			schedule.setStartEveryXMinutes(it.startEveryXMinutes)
			schedules<< schedule
		}

		return schedules;
	}

}