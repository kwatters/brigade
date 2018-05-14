package com.kmwllc.brigade.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kmwllc.brigade.logging.LoggerFactory;
import com.kmwllc.brigade.stage.AbstractStage;
import com.kmwllc.brigade.stage.Stage;
import com.kmwllc.brigade.stage.StageExceptionMode;
import com.kmwllc.brigade.workflow.Workflow;
import org.slf4j.Logger;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.kmwllc.brigade.utils.BrigadeUtils.getConfigAsString;
import static com.kmwllc.brigade.utils.BrigadeUtils.getConfigFactory;

public interface WorkflowConfig<X extends StageConfig> extends Config<WorkflowConfig>, ConfigBuilder<WorkflowConfig> {
    @JsonProperty("stages")
    List<X> getStageConfigs();

    String getName();

    int getNumWorkerThreads();

    int getQueueLength();

    void addStageConfig(X stage);

    @JsonProperty("stageExecutionMode")
    String getStageExecutionModeClass();

    @JsonIgnore
    StageExceptionMode getStageExecutionMode();

    void setStageExceptionMode(StageExceptionMode mode);

    @JsonIgnore
    List<Stage> getStages();

    Logger log = LoggerFactory.getLogger(WorkflowConfig.class.getCanonicalName());

    class EmptyWorkflowConfig implements WorkflowConfig {
        @Override
        public List getStageConfigs() {
            return null;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public int getNumWorkerThreads() {
            return 0;
        }

        @Override
        public int getQueueLength() {
            return 0;
        }

        @Override
        public void addStageConfig(StageConfig stage) {

        }

        @Override
        public String getStageExecutionModeClass() {
            return null;
        }

        @Override
        public StageExceptionMode getStageExecutionMode() {
            return null;
        }

        @Override
        public void setStageExceptionMode(StageExceptionMode mode) {

        }

        @Override
        public List<Stage> getStages() {
            return null;
        }

        @Override
        public Map<String, Object> getConfig() {
            return null;
        }

        @Override
        public void serialize(Writer w) throws ConfigException {

        }

        @Override
        public WorkflowConfig deserialize(Reader r) throws ConfigException {
            return null;
        }
    }

    default WorkflowConfig<StageConfig> buildInternal(InputStream in, Optional<BrigadeProperties> props)
            throws ConfigException {
        Optional<String> workflowString = getConfigAsString(in, props);
        if (workflowString.isPresent()) {
            String ws = workflowString.get();
            ConfigFactory configFactory = getConfigFactory(ws);
            WorkflowConfig wc = configFactory.deserializeWorkflow(ws);

            // Initialize workflow-level StageExecutionMode
            String stageExCls = wc.getStageExecutionModeClass();
            StageExceptionMode mode = StageExceptionMode.NEXT_DOC;
            if (stageExCls != null) {
                try {
                    mode = StageExceptionMode.valueOf(stageExCls);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown StageExceptionMode:  {}.  Will default to NEXT_DOC", stageExCls);
                }
            }
            wc.setStageExceptionMode(mode);

            for (Object stageConfO : wc.getStageConfigs()) {
                StageConfig stageConf = (StageConfig) stageConfO;
                String stageClass = stageConf.getStageClass().trim();
                String stageName = stageConf.getStageName();
                //log.info("Starting stage: {} class: {}", stageName, stageClass);
                try {
                    Class<?> sc = Workflow.class.getClassLoader().loadClass(stageClass);
                    AbstractStage stageInst = (AbstractStage) sc.newInstance();
                    stageInst.setName(stageName);
                    if (props.isPresent()) {
                        stageInst.setProps(props.get());
                    }

                    stageInst.init(stageConf);
                    wc.getStages().add(stageInst);
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new ConfigException("Could not instantiate stage", e);
                }
            }
            return wc;
        } else {
            throw new ConfigException("Could not get workflow serialization");
        }
    }

    @Override
    default WorkflowConfig buildFromFile(String fileName, Optional<BrigadeProperties> props)
            throws FileNotFoundException, ConfigException {
        return buildInternal(new FileInputStream(fileName), props);
    }

    @Override
    default WorkflowConfig buildFromStream(InputStream in, Optional<BrigadeProperties> props)
            throws ConfigException {
        return buildInternal(in, props);
    }

    default void appendStage(Stage s) {
        getStages().add(s);
    }

    default void insertStage(Stage s, int pos) {
        getStages().add(pos, s);
    }

    default void removeStage(String stageName) {
        Stage s = findStage(stageName);
        if (s != null) {
            getStages().remove(s);
        }
    }

    default void removeStage(int pos) {
        getStages().remove(pos);
    }

    default Stage getStage(String stageName) {
        return findStage(stageName);
    }

    default Stage getStage(int pos) {
        return getStages().get(pos);
    }

    default Stage findStage(String stageName) {
        return getStages().stream().filter(s -> s.getName().equals(stageName)).findFirst().get();
    }

    static WorkflowConfig<StageConfig> fromFile(String fileName)
            throws FileNotFoundException, ConfigException {
        return new EmptyWorkflowConfig().buildFromFile(fileName, Optional.empty());
    }

    static WorkflowConfig<StageConfig> fromFile(String fileName, BrigadeProperties bp)
            throws FileNotFoundException, ConfigException {
        return new EmptyWorkflowConfig().buildFromFile(fileName, Optional.of(bp));
    }

    static WorkflowConfig<StageConfig> fromStream(InputStream in)
            throws ConfigException {
        return new EmptyWorkflowConfig().buildFromStream(in, Optional.empty());
    }

    static WorkflowConfig<StageConfig> fromStream(InputStream in, BrigadeProperties bp)
            throws ConfigException {
        return new EmptyWorkflowConfig().buildFromStream(in, Optional.of(bp));
    }
}
