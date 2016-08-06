package com.kmwllc.brigade.stage;

import java.util.ArrayList;

import org.slf4j.Logger;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;

public class CopyFieldTest extends AbstractStageTest {

  public final static Logger log = LoggerFactory.getLogger(CopyFieldTest.class.getCanonicalName());

	@Override
	public AbstractStage createStage() {
		CopyField stage = new CopyField();
		StageConfig config = new StageConfig();
		config.setStringParam("source", "foo");
		config.setStringParam("dest", "bar");
		stage.startStage(config);
		return stage;
	}

	@Override
	public Document createDocument() {
		Document doc = new Document("1");
		doc.setField("foo", "test");
		return doc;
	}

	@Override
	public void validateDoc(Document doc) {
		log.info("{}", doc.getField("bar"));
		ArrayList<Object> vals = doc.getField("bar");
		assertEquals("test", vals.get(0));
	}

}
