package com.datatools.DataTransporter;
/*
 * 
 * Class Used for Testing
 * @author Vinod Panicker
 */
public class Content {

	private String text;

	public Content(String text) {
		this.text=text;
	}

	public boolean has(Content transferedData) {
		String tContent=transferedData.toString();
		int endIndex=tContent.length();
		int beginIndex=0;
		CharSequence charSequence =tContent.subSequence(beginIndex, endIndex);
		return this.text.contains(charSequence);
	}

	public void print() {
		System.out.println(text);
	}
	
	public String toString()
	{
		return this.text;
	}

}
