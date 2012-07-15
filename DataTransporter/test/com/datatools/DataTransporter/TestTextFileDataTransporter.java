package com.datatools.DataTransporter;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestTextFileDataTransporter {

	@Test
	public void testTextFileDataTransporter() {
		
		ContentInspector contentInspector= new ContentInspector();
		Content contentInFileone = new Content("Input");
		Content contentInFiletwo = new Content("Input");
		assertTrue(contentInspector.checkForTransferedContent(contentInFileone).in(contentInFiletwo).andReport());
	}

}
