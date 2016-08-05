package com.kmwllc.brigade.stage;

import java.util.ArrayList;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

public class CopyFieldTest extends AbstractStageTest {

	@Override
	public AbstractStage createStage() {
		// TODO Auto-generated method stub
		CopyField stage = new CopyField();
		StageConfig config = new StageConfig();
		config.setStringParam("source", "foo");
		config.setStringParam("dest", "bar");
		stage.startStage(config);
		return stage;
	}

	@Override
	public Document createDocument() {
		// TODO Auto-generated method stub
		Document doc = new Document("1");
		doc.setField("foo", "test");
		return doc;
	}

	@Override
	public void validateDoc(Document doc) {
		// TODO Auto-generated method stub
		System.out.println(doc.getField("bar"));
		ArrayList<Object> vals = doc.getField("bar");
		assertEquals("test", vals.get(0));
	}

}
