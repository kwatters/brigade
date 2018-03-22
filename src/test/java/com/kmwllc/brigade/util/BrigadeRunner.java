package com.kmwllc.brigade.util;

import com.kmwllc.brigade.Brigade;
import com.kmwllc.brigade.config.BrigadeConfig;
import com.kmwllc.brigade.config.ConnectorConfig;
import com.kmwllc.brigade.config.WorkflowConfig;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by matt on 3/22/17.
 */
public class BrigadeRunner {

    private final InputStream workflowFile;
    private final InputStream connectorFile;
    private final InputStream propertiesFile;

    public BrigadeRunner(InputStream propertiesFile, InputStream connectorFile, InputStream workflowFile) {
        this.propertiesFile = propertiesFile;
        this.connectorFile = connectorFile;
        this.workflowFile = workflowFile;
    }

    public void exec() throws Exception {
        Map<String,String> propMap = null;
        try {
            propMap = BrigadeUtils.loadPropertiesAsMap(propertiesFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String connectorXML = null;
        try {
            connectorXML = BrigadeUtils.fileToString(connectorFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String workflowXML = null;
        try {
            workflowXML = BrigadeUtils.fileToString(workflowFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StrSubstitutor sub = new StrSubstitutor(propMap);
        connectorXML = sub.replace(connectorXML);
        connectorXML = sub.replaceSystemProperties(connectorXML);
        workflowXML = sub.replace(workflowXML);
        workflowXML = sub.replaceSystemProperties(workflowXML);

        ConnectorConfig connectorConfig = ConnectorConfig.fromXML(connectorXML);
        WorkflowConfig workflowConfig = WorkflowConfig.fromXML(workflowXML);

        // init the brigade config!
        BrigadeConfig config = new BrigadeConfig();
        config.addConnectorConfig(connectorConfig);
        config.addWorkflowConfig(workflowConfig);

        // Start up the Brigade Server
        Brigade brigadeServer = Brigade.getInstance();
        brigadeServer.setConfig(config);
        try {
            brigadeServer.start();

            if (brigadeServer.isRunning()) {
                try {
                    brigadeServer.startConnector(connectorConfig.getConnectorName());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // TODO: this should do a flush! and then shutdown..
                try {
                    brigadeServer.waitForConnector(connectorConfig.getConnectorName());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            brigadeServer.shutdown(false);
            throw e;
        }

        // System.exit(0);
    }
}
