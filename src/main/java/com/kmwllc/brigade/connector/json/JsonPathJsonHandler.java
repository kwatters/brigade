package com.kmwllc.brigade.connector.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.kmwllc.brigade.config.JsonHandlerConfig;
import com.kmwllc.brigade.document.Document;
import org.apache.commons.io.input.ReaderInputStream;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by matt on 3/27/17.
 */
public class JsonPathJsonHandler implements JsonHandler {
    private String docPath;
    private Map<String, String> fieldPaths;
    private String idPattern;
    private List<String> idFields;

    @Override
    public List<Document> parseJson(Reader r) throws IOException {
        List<Document> output = new ArrayList<>();

        Configuration conf = Configuration.builder().jsonProvider(new JacksonJsonNodeJsonProvider()).build();
        ReaderInputStream ris = new ReaderInputStream(r, "UTF-8");

        Object read = JsonPath.using(conf).parse(ris).read(docPath);
        JsonNode node = (JsonNode) read;

        // We expect the docPath jsonPath expression to return either an Object or list of Objects
        Iterator<JsonNode> docIter;
        if (node instanceof ObjectNode) {
            List<JsonNode> docNodes = new ArrayList<>();
            docNodes.add(node);
            docIter = docNodes.iterator();
        } else if (node instanceof ArrayNode) {
            docIter = node.elements();
        } else {
            throw new IOException("Unexpected Json format");
        }

        // We expect the field paths to return either a ValueNode (e.g. string, int, bool) or a list of Value nodes
        while (docIter.hasNext()) {
            Document doc = new Document("temp");
            JsonNode next = docIter.next();
            for (Map.Entry<String, String> fpe:fieldPaths.entrySet()) {
                String fieldName = fpe.getKey();
                String fieldPath = fpe.getValue();
                Object fieldRead = JsonPath.using(conf).parse(next).read(fieldPath);

                if (fieldRead instanceof ValueNode) {
                    doc.setField(fieldName, ((ValueNode) fieldRead).asText());
                } else if (fieldRead instanceof ArrayNode) {
                    Iterator<JsonNode> iter2 = ((ArrayNode) fieldRead).elements();
                    while (iter2.hasNext()) {
                        JsonNode n2 = iter2.next();
                        if (n2 instanceof ValueNode) {
                            doc.addToField(fieldName, n2.asText());
                        } else {
                            throw new IOException("Unexpected Json format");
                        }
                    }
                } else {
                    throw new IOException("Unexpected Json format");
                }
            }

            List<String> idFieldValues = new ArrayList<>();
            for (String idf : idFields) {
                String val = doc.getField(idf).get(0).toString();
                idFieldValues.add(val);
            }
            String id = String.format(idPattern, idFieldValues.toArray(new String[idFieldValues.size()]));
            doc.setId(id);
            output.add(doc);
        }

        return output;
    }

    @Override
    public void setConfig(JsonHandlerConfig config) {
        docPath = config.getProperty("docPath");
        fieldPaths = config.getMapParam("fieldPaths");
        idPattern = config.getProperty("idPattern");
        idFields = config.getListParam("idFields");
    }

    public String getDocPath() {
        return docPath;
    }

    public void setDocPath(String docPath) {
        this.docPath = docPath;
    }

    public Map<String, String> getFieldPaths() {
        return fieldPaths;
    }

    public void setFieldPaths(Map<String, String> fieldPaths) {
        this.fieldPaths = fieldPaths;
    }

    public String getIdPattern() {
        return idPattern;
    }

    public void setIdPattern(String idPattern) {
        this.idPattern = idPattern;
    }

    public List<String> getIdFields() {
        return idFields;
    }

    public void setIdFields(List<String> idFields) {
        this.idFields = idFields;
    }
}
