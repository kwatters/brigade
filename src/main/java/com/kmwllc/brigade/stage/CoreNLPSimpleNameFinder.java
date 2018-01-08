package com.kmwllc.brigade.stage;

import com.google.common.base.Strings;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import edu.stanford.nlp.simple.Sentence;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.*;

/**
 * Created by matt on 4/4/17.
 */
public class CoreNLPSimpleNameFinder extends AbstractStage {

    private final static Logger log = LoggerFactory.getLogger(CoreNLPSimpleNameFinder.class.getCanonicalName());


    private static final String sep = " ";
    private Map<String, String> ioMap;
    private boolean unique = false;

    @Override
    public void startStage(StageConfig config) {
        ioMap = config.getMapParam("ioMap");
        unique = config.getBoolParam("unique", unique);
    }

    @Override
    public List<Document> processDocument(Document doc) {
        Map<String, Collection<String>> outputFieldMap = new HashMap<>();

        for (Map.Entry<String, String> entry : ioMap.entrySet()) {
            String input = entry.getKey();
            String output = entry.getValue();

            if (!doc.hasField(input)) {
                continue;
            }

            if (!outputFieldMap.containsKey(output)) {
                Collection<String> outputValue = unique ? new HashSet<String>() : new ArrayList<String>();
                outputFieldMap.put(output, outputValue);
            }

            for (Object o : doc.getField(input)) {
                String s = o.toString();
                if (Strings.isNullOrEmpty(s)) {
                    continue;
                }

                edu.stanford.nlp.simple.Document sdoc = new edu.stanford.nlp.simple.Document(s);
                for (Sentence sent : sdoc.sentences()) {
                    List<String> nerTags = sent.nerTags();

                    int i = 0;
                    Iterator<String> nerIter = nerTags.iterator();
                    List<String> currName = new ArrayList<>();
                    while (nerIter.hasNext()) {
                        String word = nerIter.next();
                        if (!word.equals("PERSON")) {
                            if (currName.size() > 0) {
                                String name = StringUtils.join(currName, sep);
                                outputFieldMap.get(output).add(name);
                            }
                            currName = new ArrayList<>();
                        } else {
                            currName.add(sent.word(i));
                        }
                        i++;
                    }
                    if (currName.size() > 0) {
                        String name = StringUtils.join(currName, sep);
                        outputFieldMap.get(output).add(name);
                    }
                }
            }
        }

        for (Map.Entry<String, Collection<String>> entry : outputFieldMap.entrySet()) {
            String fieldName = entry.getKey();
            Collection<String> fieldValues = entry.getValue();
            for (String fv : fieldValues) {
                doc.addToField(fieldName, fv);
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
