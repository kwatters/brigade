package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfiguration;
import com.kmwllc.brigade.document.Document;

public class SetStaticFieldValue extends AbstractStage {

	private String fieldName = null;
	private String value = null;
	@Override
	public void startStage(StageConfiguration config) {
		
		// TODO Auto-generated method stub
		fieldName = config.getStringParam("fieldName");
		value = config.getStringParam("value");
	}

	@Override
	public void processDocument(Document doc) {
		doc.addToField(fieldName, value);
	}

	@Override
	public void stopStage() {
		// TODO Auto-generated method stub
	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub
		
		
	}

}
