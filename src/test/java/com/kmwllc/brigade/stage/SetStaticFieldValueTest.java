package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfiguration;
import com.kmwllc.brigade.document.Document;

public class SetStaticFieldValueTest extends AbstractStageTest {

	@Override
	public AbstractStage createStage() {
		// TODO Auto-generated method stub
		SetStaticFieldValue stage = new SetStaticFieldValue();
		StageConfiguration conf = new StageConfiguration();
		conf.setStringParam("fieldName", "foo");
		conf.setStringParam("value", "bar");
		stage.startStage(conf);
		return stage;
	}

	@Override
	public Document createDocument() {
		// TODO Auto-generated method stub
		Document doc = new Document("1");
		return doc;
	}

	@Override
	public void validateDoc(Document doc) {
		// TODO Auto-generated method stub
		assertEquals(doc.getField("foo").get(0), "bar");
	}

}
