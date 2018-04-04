package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.interfaces.DocumentConnector;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.workflow.WorkflowMessage;
import com.kmwllc.brigade.workflow.WorkflowServer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.List;
import java.util.UUID;

/**
 * AbstractConnector - base class for implementing a new document connector
 * service.
 */
public abstract class AbstractConnector implements DocumentConnector {

    public final static Logger log = LoggerFactory.getLogger(AbstractConnector.class.getCanonicalName());
    protected ConnectorState state = ConnectorState.OFF;
    private int batchSize = 1;
    // TODO: batching at the connector level? (microbatching?)
    protected String docIdPrefix = "";
    protected WorkflowServer workflowServer = WorkflowServer.getInstance();
    protected String workflowName;

    private long feedCount = 0;

    private long startTime;

    private int reportModulus = 1000;

    // the current job/batch id that can be used for query delete
    private String jobId = null;
    private boolean useJobId = true;
    private String versionIdField = "version_id";

    public AbstractConnector() {
    }

    public abstract void setConfig(ConnectorConfig config);

    public void start() throws InterruptedException {

        // this is a connector job_id that is unique to this run.
        jobId = UUID.randomUUID().toString();
        log.info("Connector starting with job id {}", jobId);

        state = ConnectorState.RUNNING;
        startTime = System.currentTimeMillis();
        try {
            startCrawling();
        } catch (Exception e) {
            log.warn("Caught exception: {}", e);
            state = ConnectorState.ERROR;
        } finally {
            state = ConnectorState.STOPPED;
        }
    }

    public abstract void initialize();

    public void feed(Document doc) {
        // TODO: add batching and change this to publishDocuments (as a list)
        // Batching for this sort of stuff is a very good thing.
        feedCount++;
        if (feedCount % reportModulus == 0) {
            double feedRate = 1000.0 * feedCount / (System.currentTimeMillis() - startTime);
            log.info("Feed {} docs. Rate: {} DPS", feedCount, feedRate);
        }

        // should we add our job id to the doc?
        if (useJobId) {
            doc.setField(versionIdField, jobId);
        }

        if (!StringUtils.isEmpty(docIdPrefix)) {
            doc.setId(docIdPrefix + doc.getId());
        }

        WorkflowMessage wm = new WorkflowMessage();
        wm.setDoc(doc);
        wm.setType("add");
        wm.setWorkflow(workflowName);
        // TODO: make this call async or a thread pool  where we just put the message on a blocking queue.
        try {
            workflowServer.processMessage(wm);
        } catch (InterruptedException e) {
            log.warn("Error happened while processing message.. interrupted! {}", e);
        }

        // TODO: consider if we want batching at this level or not.
    }

    public void flush() throws Exception {
        log.info("Flush called, feed count: {}", feedCount);
        workflowServer.flush(workflowName);
    }

    public ConnectorState getState() {
        return state;
    }

    public void setState(ConnectorState state) {
        this.state = state;
    }

    public Document publishDocument(Document doc) {
        return doc;
    }

    public List<Document> publishDocuments(List<Document> batch) {
        return batch;
    }

    public ConnectorState getConnectorState() {
        return state;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getDocIdPrefix() {
        return docIdPrefix;
    }

    public void setDocIdPrefix(String docIdPrefix) {
        this.docIdPrefix = docIdPrefix;
    }

    @Override
    public void setWorkflowServer(WorkflowServer workflowServer) {
        this.workflowServer = workflowServer;
    }

    @Override
    public void setWorkflowName(String workflowName) {
        // TODO: replace this with a "topic"
        this.workflowName = workflowName;
    }

    public long getFeedCount() {
        return feedCount;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getJobId() {
        return jobId;
    }


}
