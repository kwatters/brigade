package com.kmwllc.brigade.connector;

import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.event.CallbackListener;
import com.kmwllc.brigade.event.ConnectorEventListener;
import com.kmwllc.brigade.event.DocumentListener;
import com.kmwllc.brigade.interfaces.DocumentConnector;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.stage.StageFailure;
import com.kmwllc.brigade.workflow.WorkflowMessage;
import com.kmwllc.brigade.workflow.WorkflowServer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * AbstractConnector - base class for implementing a new document connector
 * service.<br/><br/>
 * The AbstractConnector maintains a list of ConnectorEventListeners which listen for the start and/or
 * stop of the Connector.  Connector instances may add/remove listeners either statically (through configuration
 * file) or programmatically.<br/><br/>
 * AbstractConnector also implements DocumentListener and CallbackListener and will be notified when documents
 * complete the pipeline or fail due to an exception being thrown.  Connector instances are automatically wired
 * to listen to events on their executing workflow.  On receiving DocumentListener or CallbackListener events,
 * the AbstractConnector delegates the event to any registered listeners.  A list of DocumentListeners and
 * CallbackListeners is maintained.  Like the ConnectorEventListeners, these listeners may be added/removed
 * either through a configuration file or programmatically.
 */
public abstract class AbstractConnector implements DocumentConnector, DocumentListener, CallbackListener {

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

    private List<ConnectorEventListener> connectorEventListeners = new ArrayList<>();
    private List<DocumentListener> documentListeners = new ArrayList<>();
    private List<CallbackListener> callbackListeners = new ArrayList<>();

    public AbstractConnector() {
    }

    /**
     * Set properties on the connector based upon the ConnectorConfig.  The base method will add connector
     * listeners that are specified in the config.  Subclasses implementing this method should be sure
     * to call super.setConfig() to get this.
     * <p>
     * This method should not be called directly by client code as it is called during standard Brigade
     * execution.  In future versions, it should have reduced (package-private?) visibility.
     * @param config Configuration to use
     */
    public void setConfig(ConnectorConfig config) {
        connectorEventListeners.addAll(config.getConnectorListeners());
        documentListeners.addAll(config.getConnectorListeners());
        callbackListeners.addAll(config.getConnectorListeners());
    }

    /**
     * Start the connector.  This will invoke the startCrawling() method on the connector.
     * While the connector is running, it's state is set to RUNNING.  If the connector completes without
     * interruption, the state is set to STOPPED.  If it catches an exception the state is set to
     * ERROR and an InterruptedException is thrown.  Regardless of outcome, a ConnectorEnd event is fired
     * before this method returns.
     * <p>
     * This method should not be called directly by client code.  It is automatically invoked during standard
     * Brigade execution.  In future versions, it should have reduced (package-private?) visibility.
     * @throws InterruptedException If an exception was caught during connector run.
     */
    public void start() throws InterruptedException {

        // this is a connector job_id that is unique to this run.
        jobId = UUID.randomUUID().toString();
        log.info("Connector starting with job id {}", jobId);

        state = ConnectorState.RUNNING;
        startTime = System.currentTimeMillis();
        try {
            startCrawling();
            state = ConnectorState.STOPPED;
        } catch (Exception e) {
            log.warn("Caught exception: {}", e);
            state = ConnectorState.ERROR;
            throw new InterruptedException("Connector interrupted due to exception");
        } finally {
            fireConnectorEnd();
        }
    }

    void fireConnectorBegin(ConnectorConfig cc) {
        connectorEventListeners.forEach(l -> l.connectorBegin(cc));
    }

    void fireConnectorEnd() {
        connectorEventListeners.forEach(l -> l.connectorEnd());
    }

    @Override
    public void docComplete(String docId) {
        callbackListeners.forEach(l -> l.docComplete(docId));
    }

    @Override
    public void docFail(String docId, List<StageFailure> failures) {
        callbackListeners.forEach(l -> l.docFail(docId, failures));
        state = ConnectorState.STOPPED;
    }

    @Override
    public void onDocument(Document doc) {
        documentListeners.forEach(l -> l.onDocument(doc));
    }

    /**
     * Method to handle any additional work that needs to be done to prepare a connector after reading
     * its configuration.  Default is no-op.  This method should not be called directly by client code.
     */
    public abstract void initialize();

    /**
     * Feed the document into the Workflow.  Connectors should call this method once they have finished
     * creating a document to be processed.  The method wraps the document in a WorkflowMessage and hands
     * this to the workflowServer which queues it up for the pipeline worker threads.
     * @param doc Document to be fed to the workflow
     */
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

    /**
     * Flush the workflow, processing any remaining documents.  This should be called by connectors, usually
     * from the Connector.flush() method. This method just logs a message and tells the workflow server to flush
     * this workflow.
     * @throws Exception if an exception occurred during workflow flush
     */
    public void flush() throws Exception {
        log.info("Flush called, feed count: {}", feedCount);
        workflowServer.flush(workflowName);
    }

    /**
     * Get the processing state of the connector.
     * @return State of Connector
     */
    public ConnectorState getState() {
        return state;
    }

    /**
     * Set the processing state of the connector to the specified value.
     * @param state Connector state
     */
    public void setState(ConnectorState state) {
        this.state = state;
    }

    /**
     * Not currently implemented
     * @param doc
     * @return
     */
    public Document publishDocument(Document doc) {
        return doc;
    }

    /**
     * Not currently implemented
     * @param batch
     * @return
     */
    public List<Document> publishDocuments(List<Document> batch) {
        return batch;
    }

    /**
     * Get processing state of Connector
     * @return Connector state
     */
    public ConnectorState getConnectorState() {
        return state;
    }

    /**
     * Not yet implemented
     * @return
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Not yet implemented
     * @param batchSize
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Get prefix to add to document id
     * @return Prefix to add to document id
     */
    public String getDocIdPrefix() {
        return docIdPrefix;
    }

    /**
     * Set prefix for document id to the specified value
     * @param docIdPrefix Prefix to apply
     */
    public void setDocIdPrefix(String docIdPrefix) {
        this.docIdPrefix = docIdPrefix;
    }

    /**
     * @param workflowServer
     */
    @Override
    public void setWorkflowServer(WorkflowServer workflowServer) {
        this.workflowServer = workflowServer;
    }

    @Override
    public void setWorkflowName(String workflowName) {
        // TODO: replace this with a "topic"
        this.workflowName = workflowName;
    }

    /**
     * Get number of documents the connector has fed to the pipeline
     * @return Number of docs
     */
    public long getFeedCount() {
        return feedCount;
    }

    /**
     * Get time (in epochal millis) that the connector started
     * @return Start time (in millis) for connector
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Get the jobId that was randomly assigned to this connector execution.  This is used for versioning
     * of documents; all documents inserted during a connector execution will have the same jobId.
     * @return Job id for the connector.
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * Get the list of ConnectorEventListeners registered for this connector
     * @return List of listeners
     */
    public List<ConnectorEventListener> getConnectorEventListeners() {
        return connectorEventListeners;
    }

    /**
     * Set the list of ConnectorEventListeners to the one specified.  Usually, one would instead add a
     * ConnectorListener as that extends ConnectorEventListener
     * @param connectorEventListeners List of listeners
     */
    public void setConnectorEventListeners(List<ConnectorEventListener> connectorEventListeners) {
        this.connectorEventListeners = connectorEventListeners;
    }

    /**
     * Add a ConnectorEventListener to the list of ConnectorEventListeners maintained by the connector.
     * Usually, one would instead add a ConnectorListener as that extends ConnectorEventListener
     * @param l Listener to add
     */
    public void addConnectorEventListener(ConnectorEventListener l) {
        connectorEventListeners.add(l);
    }

    /**
     * Remove the specified ConnectorEventListener from the list maintained by the connector.
     * @param l Listener to remove
     */
    public void removeConnectorEventListener(ConnectorEventListener l) {
        connectorEventListeners.remove(l);
    }
}
