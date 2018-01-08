package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.List;

/**
 * This stage will delete a field on the document.
 *
 * @author kwatters
 */
public class DeleteFields extends AbstractStage {

    private List<String> fields;

    @Override
    public void startStage(StageConfig config) {
        fields = config.getListParam("fields");
    }

    @Override
    public List<Document> processDocument(Document doc) {
        for (String fieldName : fields) {
            if (doc.hasField(fieldName)) {
                doc.removeField(fieldName);
            }
        }
        return null;
    }

    @Override
    public void stopStage() {
        // no-op
    }

    @Override
    public void flush() {
        // no-op
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFieldName(List<String> fields) {
        this.fields = fields;
    }

}
