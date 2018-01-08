package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This stage will iterate the field values and see if they are a match in a csv based dictionary.
 * The matches are then output into the output field.
 *
 * @author kwatters
 */
public class DictionaryLookup extends AbstractStage {

    public final static Logger log = LoggerFactory.getLogger(DictionaryLookup.class.getCanonicalName());

    private Map<String, String> ioMap;

    private String dictionaryFile;
    private HashMap<String, List<String>> dictionary;
    private String defaultAsInputField = "false";
    private String defaultValue = "Unknown";

    @Override
    public void startStage(StageConfig config) {
        if (config != null) {
            ioMap = config.getMapParam("ioMap");
            defaultAsInputField = config.getProperty("defaultAsInputField", "false");
            dictionaryFile = config.getProperty("dictionaryFile", "mydict.csv");

        }

        try {
            dictionary = DictionaryLoader.getInstance().loadDictionary(dictionaryFile);
        } catch (IOException e) {
            log.warn("Error loading dictionary {} IOException {}", dictionaryFile, e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public List<Document> processDocument(Document doc) {
    /*
     * input field values: I1, X1, I2, Y1, I3 output fields: out1, out2, out3
     * dict: I1, A1, B1, C1 I2, A2, B2, C2 I3, A3, B3, C3
     * 
     * Result: out1: A1, A2, A3 out2: B1, B2, B3 out3: C1, C2, C3
     */
        for (Map.Entry<String, String> e : ioMap.entrySet()) {
            String inputField = e.getKey();
            String outputField = e.getValue();
            if (!doc.hasField(inputField)) {
                continue;
            }

            ArrayList<List<String>> lookedupValues = new ArrayList<>();
            ArrayList<String> defaults = new ArrayList<String>();
            for (Object o : doc.getField(inputField)) {
                if (o == null) {
                    continue;
                }
                List<String> dictCols = dictionary.get(o.toString());
                if (dictCols != null) {
                    lookedupValues.add(dictCols);
                } else {
                	if (defaultAsInputField.equals("true")) {
                	  defaults.add(o.toString());
                  }
                }
            }

            if (inputField.equals(outputField)) {
                doc.removeField(outputField);
            }            
            
            if (defaults.size() > 0) {
              for (String value : defaults) {
                doc.addToField(outputField, value);
              }
            }
            
            for (List<String> dictCols : lookedupValues) {
                for (String val : dictCols) {
                    if (!StringUtils.isEmpty(val)) {
                        doc.addToField(outputField, val);
                    } else {
                        // handle an empty value?
                        if (defaultValue != null) {
                            doc.addToField(outputField, defaultValue);
                        }
                    }
                }
            }
        }

        // this stage doesn't emit child docs.
        return null;
    }

    @Override
    public void stopStage() {

    }

    @Override
    public void flush() {

    }

}
