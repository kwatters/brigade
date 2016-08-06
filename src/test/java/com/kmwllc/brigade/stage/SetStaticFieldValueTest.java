package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

public class SetStaticFieldValueTest extends AbstractStageTest {

	@Override
	public AbstractStage createStage() {
		SetStaticFieldValue stage = new SetStaticFieldValue();
		StageConfig conf = new StageConfig();
		conf.setStringParam("fieldName", "foo");
		conf.setStringParam("value", "bar");
		stage.startStage(conf);
		return stage;
	}

	@Override
	public Document createDocument() {
		// create a simple test document
		Document doc = new Document("1");
		return doc;
	}

	@Override
	public void validateDoc(Document doc) {
		assertEquals(doc.getField("foo").get(0), "bar");
	}

}
