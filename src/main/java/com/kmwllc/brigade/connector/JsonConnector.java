package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.JsonHandlerConfig;
import com.kmwllc.brigade.connector.json.JsonHandler;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * Created by matt on 3/24/17.
 */
public class JsonConnector extends AbstractConnector {
    private String jsonHandler;
    private String baseAddress;
    private JsonHandler handler;
    private String docPath;
    private Map<String, String> fieldPaths;

    // TODO refactor pagination to a delegate
    private boolean paginated;
    private int pageStart;
    private int pageIncrement;
    private int pageStop;
    private String paginationPattern;

    public final static Logger log = LoggerFactory.getLogger(JsonConnector.class.getCanonicalName());

    @Override
    public void startCrawling() throws Exception {
        if (paginated) {
            int currPage = pageStart;
            int numResults = 0;
            int stopAt = (pageStop >= 0) ? pageStop : Integer.MAX_VALUE;

            do {
                Reader jsonReader = null;
                try {
                    jsonReader = getJson(currPage++);
                } catch (IOException e) {
                    e.printStackTrace();
                    log.warn("Error in json connector: {}", e);
                }
                List<Document> results = null;
                try {
                    results = handler.parseJson(jsonReader);
                } catch (IOException e) {
                    e.printStackTrace();
                    log.warn("Error parsing json: {}", e);
                }
                numResults = results.size();
                if (results.size() > 0) {
                    for (Document doc : results) {
                        feed(doc);
                    }
                }
            } while (numResults > 0 && currPage != stopAt);
        }

        flush();
    }

    private Reader getJson(int page) throws IOException {
        log.info(String.format("Get page: %d", page));
        // TODO: Streaming for large Json files
        HttpClient client = HttpClientBuilder.create().build();
        String getAddress = baseAddress;
        if (paginated) {
            String pageParam = String.format(paginationPattern, page);
            getAddress = String.format("%s%s", getAddress, pageParam);
        }
        HttpGet get = new HttpGet(getAddress);
        HttpResponse resp = client.execute(get);
        BufferedReader br = new BufferedReader(new InputStreamReader(resp.getEntity().getContent()));
        StringBuilder sb = new StringBuilder();
        String s;
        while ((s = br.readLine()) != null) {
            sb.append(s);
        }
        br.close();
        return new StringReader(sb.toString());
    }

    @Override
    public void stopCrawling() {

    }

    @Override
    public void setConfig(ConnectorConfig config) {
        workflowName = config.getProperty("workflowName");
        baseAddress = config.getProperty("baseAddress");
        paginated = config.getBoolParam("paginated", false);
        pageStart = config.getIntegerParam("pageStart", 1);
        pageIncrement = config.getIntegerParam("pageIncrement", 1);
        pageStop = config.getIntegerParam("pageStop", -1);
        paginationPattern = config.getProperty("paginationPattern");
        JsonHandlerConfig jhconfig = (JsonHandlerConfig) config.getObjectParam("jsonHandler");
        jsonHandler= jhconfig.getProperty("className");
        try {
            Class<JsonHandler> handlerClass = (Class<JsonHandler>) Class.forName(jsonHandler);
            handler = handlerClass.newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.warn(String.format("Could not find class: %s", jsonHandler), e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            log.warn(String.format("Could not access class: %s", jsonHandler), e);
        } catch (InstantiationException e) {
            e.printStackTrace();
            log.warn(String.format("Could not instantiate class: %s", jsonHandler), e);
        }
        handler.setConfig(jhconfig);
    }

    @Override
    public void initialize() {

    }
}
