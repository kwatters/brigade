package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Created by matt on 4/4/17.
 */
public class NameFinder extends AbstractStage {

    private final static Logger log = LoggerFactory.getLogger(OpenNLP.class.getCanonicalName());

    private final String tokenizerModel = "en-token.bin";
    private final String personModel = "en-ner-person.bin";
    private final String sep = " ";

    // NB: not thread safe
    private Tokenizer tokenizer;
    private NameFinderME nameFinder;

    private Map<String, String> ioMap;
    private boolean unique = false;

    @Override
    public void startStage(StageConfig config) {
        ioMap = config.getMapParam("ioMap");
        unique = config.getBoolParam("unique", unique);

        try {
            TokenizerModel tokenModel = new TokenizerModel(NameFinder.class.getClassLoader().
                    getResourceAsStream(tokenizerModel));
            tokenizer = new TokenizerME(tokenModel);

            TokenNameFinderModel nameModel = new TokenNameFinderModel(NameFinder.class.getClassLoader().
                    getResourceAsStream(personModel));
            nameFinder = new NameFinderME(nameModel);
        } catch (IOException e) {
            log.warn("Error loading OpenNLP models {}", e.getLocalizedMessage());
        }
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

                String[] tokens = tokenizer.tokenize(s);
                Span[] spans = nameFinder.find(tokens);
                for (Span span : spans) {
                    String[] terms = Arrays.copyOfRange(tokens, span.getStart(), span.getEnd());
                    String entity = StringUtils.join(terms, sep);
                    outputFieldMap.get(output).add(entity);
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
