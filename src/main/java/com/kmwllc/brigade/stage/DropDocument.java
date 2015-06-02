package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfiguration;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.document.ProcessingStatus;

/**
 * DropDocument - if the document contains a particular field value, drop this document from 
 * the workflow.
 * input  field
 * input  value
 * 
 * @author kwatters
 *
 */
public class DropDocument extends AbstractStage {

	private String field;
	private String value;
	
	@Override
	public void startStage(StageConfiguration config) {
		// TODO Auto-generated method stub
		field = config.getProperty("field", null);
		value = config.getProperty("value", null);
	}

	@Override
	public void processDocument(Document doc) {
		// TODO Auto-generated method stub
		if (doc.hasField(field))  {
			for (Object o : doc.getField(field)) {
				if (o.equals(value)) {
					doc.setStatus(ProcessingStatus.DROP);
					break;
				}
			}
		}
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
