package com.datatools.DataTransporter


public class DebugConfigHelper {

	public static String print(ArrayList cfgs) {
		String result="";
		cfgs.each{
			result=result+it.print()
		}
		return result;
	}

}