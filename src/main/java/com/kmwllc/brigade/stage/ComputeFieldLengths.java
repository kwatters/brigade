package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;
import java.util.Map;

/**
 * This stage will compute the character length of string fields on a document
 * 
 * @author cclemente
 *
 */
public class ComputeFieldLengths extends AbstractStage {

	private Map<String, String> fieldNameMap;

	@Override
	public void startStage(StageConfig config) {
		fieldNameMap = config.getMapParam("fieldNameMap");
	}

	@Override
	public List<Document> processDocument(Document doc) {
		for (String source : fieldNameMap.keySet()) {
			if (!doc.hasField(source)) {
				continue; // field not found, skip
			}
			String dest = fieldNameMap.get(source); // should be *_length
			try {
				String sourceText = doc.getField(source).get(0).toString();
				int textLength = sourceText.length();
				doc.addToField(dest, textLength);
			} catch (Exception e) {
				log.warn("Error computing length of field '" + source + "': ", e);
			}
		}
		return null;
	}

	@Override
	public void stopStage() {
		// NO-OP
	}

	@Override
	public void flush() {
		// NO-OP
	}

}
