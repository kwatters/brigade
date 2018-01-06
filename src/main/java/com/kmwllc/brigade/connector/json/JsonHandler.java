package com.kmwllc.brigade.connector.json;

import com.kmwllc.brigade.config.JsonHandlerConfig;
import com.kmwllc.brigade.document.Document;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * Created by matt on 3/24/17.
 */
public interface JsonHandler {
    List<Document> parseJson(Reader r) throws IOException;
    void setConfig(JsonHandlerConfig config);
}
