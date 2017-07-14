package com.kmwllc.brigade.stage.dict;

import java.io.InputStream;
import java.util.List;

/**
 * Created by matt on 4/19/17.
 */
public interface DictionaryManager {
    void loadDictionary(InputStream in);
    boolean hasTokens(List<String> t);
    EntityInfo getEntity(List<String> t);
}
