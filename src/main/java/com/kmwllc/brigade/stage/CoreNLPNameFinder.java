package com.kmwllc.brigade.stage;

import com.google.common.base.Strings;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.*;

/**
 * Created by matt on 4/4/17.
 */
public class CoreNLPNameFinder extends AbstractStage {

    private final static Logger log = LoggerFactory.getLogger(CoreNLPNameFinder.class.getCanonicalName());


    private static final String sep = " ";
    private static final String PERSON = "PERSON";
    private Map<String, String> ioMap;
    private boolean unique = false;
    private StanfordCoreNLP pipeline;

    @Override
    public void startStage(StageConfig config) {
        ioMap = config.getMapParam("ioMap");
        unique = config.getBoolParam("unique", unique);

        Properties p = new Properties();
        p.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner");
        pipeline = new StanfordCoreNLP(p);
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

                Annotation annot = new Annotation(s);
                pipeline.annotate(annot);
                List<CoreMap> sentences = annot.get(CoreAnnotations.SentencesAnnotation.class);
                for (CoreMap sent : sentences) {
                    List<String> currName = new ArrayList<>();
                    for (CoreLabel tok : sent.get(CoreAnnotations.TokensAnnotation.class)) {
                        String ne = tok.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                        if (!ne.equals(PERSON)) {
                            if (currName.size() > 0) {
                                String name = StringUtils.join(currName, sep);
                                outputFieldMap.get(output).add(name);
                            }
                            currName = new ArrayList<>();
                        } else {
                            currName.add(tok.word());
                        }
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
