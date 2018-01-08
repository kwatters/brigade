package com.kmwllc.brigade.stage;

import com.google.common.base.Strings;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * This stage will copy field values from field A to field B.
 *
 * @author kwatters
 */
public class CopyEitherFieldToId extends AbstractStage {
    private List<String> fields = new ArrayList<>();

    @Override
    public void startStage(StageConfig config) {
        if (config != null) {
            fields = config.getListParam("fields");
        }
    }

    @Override
    public List<Document> processDocument(Document doc) {
        // Set first available
        for (String f : fields) {
            if (doc.hasField(f) && !Strings.isNullOrEmpty((String) doc.getField(f).get(0))) {
                doc.setId((String) doc.getField(f).get(0));

            } else {
                // noop this doc doesn't have the field. ignore it.
            }
        }
        return null;
    }

    @Override
    public void stopStage() {
        // no-op for this stage
    }

    @Override
    public void flush() {
        // no-op for this stage
    }

}
