package com.datatools.DataTransporter;

public class ContentInspector {

	private Content transferedData;
	private Content dataForInspection;


	private void setTransferedContent(Content transferedData) {
		this.transferedData=transferedData;
	}

	public ContentInspector in(Content dataForInspection) {
		setDataForInspection(dataForInspection);
		return this;
	}

	private void setDataForInspection(Content dataForInspection) {
		this.dataForInspection=dataForInspection;
	}

	public ContentInspector checkForTransferedContent(Content contentInFileone) {
		setTransferedContent(contentInFileone);
		return this;
	}

	public boolean andReport() {
		dataForInspection.print();
		dataForInspection.print();
		return dataForInspection.has(transferedData);
	}

}
