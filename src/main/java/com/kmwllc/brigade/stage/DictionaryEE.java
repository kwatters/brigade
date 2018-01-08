package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.dict.DictionaryManager;
import com.kmwllc.brigade.stage.dict.EntityInfo;
import com.kmwllc.brigade.stage.dict.FSTDictionaryManager;
import com.kmwllc.brigade.stage.dict.PatriciaTrieDictionaryManager;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by matt on 4/19/17.
 */
public class DictionaryEE extends AbstractStage {

    private String inputField;
    private String outputField;
    private String dictionaryFile;
    private String dictionaryManager = "FST";
    private DictionaryManager dictMgr = new PatriciaTrieDictionaryManager();
    private static Tokenizer tokenizer = SimpleTokenizer.INSTANCE;

    @Override
    public void startStage(StageConfig config) {
        inputField = config.getStringParam("inputField");
        outputField = config.getStringParam("outputField");
        dictionaryFile = config.getStringParam("dictionaryFile");
        dictionaryManager = config.getStringParam("dictionaryManager", dictionaryManager);

        switch (dictionaryManager) {
            case "FST":
                dictMgr = new FSTDictionaryManager();
                break;
            case "Trie":
                dictMgr = new PatriciaTrieDictionaryManager();
                break;
            default:
                throw new RuntimeException(String.format("Dictionary Manager (%s) is not supported",
                        dictionaryManager));
        }

        try {
            dictMgr.loadDictionary(new FileInputStream(dictionaryFile));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Document> processDocument(Document doc) throws Exception {
        String input = (String) doc.getField(inputField).get(0);
        String[] tokens = tokenizer.tokenize(input);
        Set<EntityInfo> infos = new HashSet<>();

        for (int i = 0; i < tokens.length; i++) {
            // find longest match
            int mark = i;
            List<String> curr = new ArrayList<>();
            do {
                curr.add(tokens[mark++]);
            } while (dictMgr.hasTokens(curr) && mark < tokens.length);
            List<String> longestMatch = (mark == tokens.length) ? curr : curr.subList(0, curr.size() - 1);

            EntityInfo ei = dictMgr.getEntity(longestMatch);
            if (ei != null) {
                infos.add(ei);
            }
        }

        Set<String> outputs = new HashSet<>();
        for (EntityInfo ei : infos) {
            outputs.addAll(ei.getPayloads());
        }
        for (String payload : outputs) {
            doc.addToField(outputField, payload);
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
