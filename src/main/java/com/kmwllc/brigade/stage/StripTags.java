package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.apache.commons.lang3.StringEscapeUtils;
import org.elasticsearch.common.Strings;
import org.jsoup.Jsoup;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Created by matt on 3/31/17.
 */
public class StripTags extends AbstractStage {

    private Map<String, String> ioMap;
    public final static Logger log = LoggerFactory.getLogger(StripTags.class.getCanonicalName());

    @Override
    public void startStage(StageConfig config) {
        ioMap = config.getMapParam("ioMap");
    }

    @Override
    public List<Document> processDocument(Document doc) {
        for (Map.Entry<String, String> entry : ioMap.entrySet()) {
            String inputField = entry.getKey();
            String outputField = entry.getValue();
            boolean inplace = false;
            if (Strings.isNullOrEmpty(outputField) || inputField.equals(outputField)) {
                inplace = true;
            }
            if (!doc.hasField(inputField)) {
                continue;
            }
            for (Object fvo : doc.getField(inputField)) {
                String fv = (String) fvo;

                fv = StringEscapeUtils.unescapeXml(fv);
                fv = StringEscapeUtils.unescapeJava(fv);

                String writeField = inplace ? String.format("temp_%s", inputField) : outputField;

                // Possibly regex (<[^>]+>) is good enough here?
                org.jsoup.nodes.Document document = Jsoup.parse(fv);
                document.select("*").append(" ");
                doc.addToField(writeField, document.text());
            }
            if (inplace) {
                String tempField = String.format("temp_%s", inputField);
                doc.setField(inputField, doc.getField(tempField));
                doc.removeField(tempField);
            }
        }

        return null;
    }

    @Override
    public void stopStage() {

    }

    @Override
    public void flush() {

    }
}
