package com.kmwllc.brigade.util;

import com.kmwllc.brigade.config.*;
import com.kmwllc.brigade.utils.BrigadeRunner;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.util.Optional;

/**
 * Created by matt on 3/27/17.
 */
public class BrigadeHelper extends ExternalResource {

    private BrigadeRunner brigadeRunner;
    private Optional<String> propertiesFile = Optional.empty();
    private Optional<String> connectorFile = Optional.empty();
    private Optional<String> workflowFile = Optional.empty();
    private Optional<BrigadeProperties> properties = Optional.empty();
    private Optional<ConnectorConfig> connector = Optional.empty();
    private Optional<WorkflowConfig<StageConfig>> workflow = Optional.empty();

    public BrigadeHelper(String propertiesFile, String connectorFile, String workflowFile) {
        try {
            init(propertiesFile, connectorFile, workflowFile);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ConfigException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        if (properties.isPresent() && connector.isPresent() && workflow.isPresent()) {
            init(properties.get(), connector.get(), workflow.get());
        } else if (propertiesFile.isPresent() && connectorFile.isPresent() && workflowFile.isPresent()) {
            init(propertiesFile.get(), connectorFile.get(), workflowFile.get());
        }
    }

    public void init(String propertiesFile, String connectorFile, String workflowFile)
            throws IOException, ConfigException {
        brigadeRunner = BrigadeRunner.init(propertiesFile, connectorFile, workflowFile);
    }

    public void init(BrigadeProperties bp, ConnectorConfig cc, WorkflowConfig<StageConfig> wc) {
        brigadeRunner = new BrigadeRunner(bp, cc, wc);
    }

    @Override
    protected void after() {
        // Nothing to do?
        super.after();
    }

    public void exec() throws Exception {
        brigadeRunner.exec();
    }
}
