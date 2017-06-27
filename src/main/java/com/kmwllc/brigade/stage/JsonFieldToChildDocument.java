package com.kmwllc.brigade.stage;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;
import org.slf4j.Logger;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;


public class JsonFieldToChildDocument extends AbstractStage {

	public final static Logger log = LoggerFactory.getLogger(JsonFieldToChildDocument.class.getCanonicalName());
	private String jsonField = "json";
	private String childTable = "child";
	private String parentIdField = "parent_id";
	
	@Override
	public void startStage(StageConfig config) {
		jsonField = config.getStringParam("jsonField", jsonField);
		childTable = config.getStringParam("childTable", childTable);
		parentIdField = config.getStringParam("parentIdField", parentIdField);
	}

	@Override
	public List<Document> processDocument(Document doc) {
		// TODO Auto-generated method stub
		if (!doc.hasField(jsonField)) {
			return null;
		}
		ArrayList<Document> childrenDocs = new ArrayList<Document>();
		// create child docs with integer offset increasing
		int childId = 0;
		String parentId = doc.getId();
		for (Object o : doc.getField(jsonField)) {
			childId++;
			Document child = new Document(childTable + "_"+ parentId + "_" + childId);
			child.setField(parentIdField, parentId);
			child.setField("table", childTable);
			try {
				// TODO: handle deeper nestings of json docs.
				JSONObject obj = new JSONObject(o.toString());
				for (String key : obj.keySet()) {
					//log.info("JSON Key : {} VALUE : {}", key, obj.get(key));
					child.addToField(key, obj.get(key));
				}
			} catch (Exception e) {
				log.warn("JSON Parse error on doc {}", doc.getId());
				// track the error?  TODO: make this configurable.
				child.setField("json_error", e.getMessage());
				// continue;
			}
			childrenDocs.add(child);
		}
		if (childrenDocs.size() > 0) {
			return childrenDocs;
		} else { 
			return null;
		}
	}

	@Override
	public void stopStage() {
		// NoOp
	}

	@Override
	public void flush() {
		// NoOp
	}

}
