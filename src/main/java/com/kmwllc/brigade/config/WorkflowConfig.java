package com.kmwllc.brigade.config;

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

/**
 * Interface for Workflow configurations.  Also has static methods to build that can be called on
 * the interface: e.g. WorkflowConfig.fromFile().  The actual building is handled by the ConfigFactory
 * which builds config objects based on detecting the format from the file or stream.<br/><br/>
 * WorkflowConfig supports having Listeners added and removed dynamically.
 * @param <X> Type of StageConfigs to build
 */
public interface WorkflowConfig<X extends StageConfig> extends Config<WorkflowConfig>, ConfigBuilder<WorkflowConfig> {
    /**
     * Get list of StageConfigs that make up the workflow
     * @return List of StageConfigs
     */
    List<X> getStageConfigs();

    /**
     * Get the name of the workflow
     * @return Name of the workflow
     */
    String getName();

    /**
     * Get number of worker threads that will be used to process the pipeline
     * @return Number of threads
     */
    int getNumWorkerThreads();

    /**
     * Get size of queue (in number of documents) used to store documents delivered by the connector
     * @return Size of queue
     */
    int getQueueLength();

    /**
     * Add a stage config to the pipeline
     * @param stage
     */
    void addStageConfig(X stage);

    /**
     * Get the name of the StageExceptionMode enum that is used to determine handling of exceptions
     * thrown by stages in the pipeline
     * @return One of NEXT_DOC, NEXT_STAGE, STOP_WORKFLOW
     */
    String getStageExecutionModeClass();

    /**
     * Get the StageExceptionMode that is set as the default for the pipeline.  Note that this can be overridden
     * by individual stages.  If this is not specified for the pipeline, the default setting is NEXT_DOC
     * @return StageExceptionMode for the pipeline
     */
    StageExceptionMode getStageExecutionMode();

    /**
     * Set the StageExceptionMode for the pipeline.  Note that this can be overridden by individual stages.
     * If this is not specified for the pipeline, the default setting is NEXT_DOC
     * @param mode Mode to set StageExceptionMode to for the pipeline
     */
    void setStageExceptionMode(StageExceptionMode mode);

    /**
     * Get List of populated Stages for the pipeline
     * @return List of populated stages
     */
    List<Stage> getStages();

    Logger log = LoggerFactory.getLogger(WorkflowConfig.class.getCanonicalName());

    // Workaround so that we can support both polymorphism and static invocation of WorkflowConfig
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

    /**
     * Intended as internal constructor; should not be called by client code.  When we move to Java 9, this
     * method should be made private.  Reads config from stream; uses reflection to establish StageExceptionMode
     * and to instantiate stages.
     * @param in Stream to build WorkflowConfig from
     * @param props If present, BrigadeProperties to perform variable expansion against
     * @return A populated WorkflowConfig
     * @throws ConfigException If Workflow could not be instantiated
     */
    default WorkflowConfig<StageConfig> buildInternal(InputStream in, Optional<BrigadeProperties> props)
            throws ConfigException {
        Optional<String> workflowString = getConfigAsString(in, props);
        if (workflowString.isPresent()) {
            String ws = workflowString.get();
            ConfigFactory configFactory = getConfigFactory(ws);
            WorkflowConfig wc = configFactory.deserializeWorkflow(ws);

            // Initialize workflow-level StageExecutionMode
            // Individual stages may override this
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

            // Initialize stages now so that we have a list of stage instances we can add/remove to dynamically
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

    /**
     * Add stage to list of stages to be executed by the workflow
     * @param s Stage to add.
     */
    default void appendStage(Stage s) {
        getStages().add(s);
    }

    /**
     * Insert stage at specified position in list of stages to be executed by the workflow
     * @param s Stage to add
     * @param pos Position to insert the stage
     */
    default void insertStage(Stage s, int pos) {
        getStages().add(pos, s);
    }

    /**
     * Remove stage identified by specified name from the list of stages to be executed by the workflow
     * @param stageName Name of stage to remove
     */
    default void removeStage(String stageName) {
        Stage s = findStage(stageName);
        if (s != null) {
            getStages().remove(s);
        }
    }

    /**
     * Remove stage at specified position from the list of stages to be executed by the workflow.
     * @param pos Position of stage to remove
     */
    default void removeStage(int pos) {
        getStages().remove(pos);
    }

    /**
     * Get the stage identified by specified name
     * @param stageName Name of stage to get
     * @return Stage instance
     */
    default Stage getStage(String stageName) {
        return findStage(stageName);
    }

    /**
     * Get the stage that is in the specified position in the list of stages
     * @param pos Position of stage to get
     * @return Stage instance
     */
    default Stage getStage(int pos) {
        return getStages().get(pos);
    }

    /**
     * Intended as internal method only.  In Java 9, this should be a private method.
     * Find stage in list of stages that has specified name. This is used behind the scenes
     * by getStage and removeStage.
     * @param stageName Name of stage
     * @return Stage instance
     */
    default Stage findStage(String stageName) {
        return getStages().stream().filter(s -> s.getName().equals(stageName)).findFirst().get();
    }

    /**
     * Build WorkflowConfig from specified file.  Variable expansion will not be performed
     * @param fileName Path to file
     * @return A populated WorkflowConfig
     * @throws FileNotFoundException if could not read file
     * @throws ConfigException if WorkflowConfig could not be instantiated
     */
    static WorkflowConfig<StageConfig> fromFile(String fileName)
            throws FileNotFoundException, ConfigException {
        return new EmptyWorkflowConfig().buildFromFile(fileName, Optional.empty());
    }

    /**
     * Build Workflowconfig from specified file.  Perform variable expansion against the
     * specified BrigadeProperties instance
     * @param fileName Path to file
     * @param bp BrigadeProperties instance to perform variable expansion against
     * @return A populated WorkflowConfig
     * @throws FileNotFoundException if there was an error reading the file
     * @throws ConfigException if the WorkflowConfig could not be instantiated
     */
    static WorkflowConfig<StageConfig> fromFile(String fileName, BrigadeProperties bp)
            throws FileNotFoundException, ConfigException {
        return new EmptyWorkflowConfig().buildFromFile(fileName, Optional.of(bp));
    }

    /**
     * Build WorkflowConfig from specified stream.  No variable expansion will be performed
     * @param in Stream to build from
     * @return A populated WorkflowConfig
     * @throws ConfigException if the WorkflowConfig could not be instantiated
     */
    static WorkflowConfig<StageConfig> fromStream(InputStream in)
            throws ConfigException {
        return new EmptyWorkflowConfig().buildFromStream(in, Optional.empty());
    }

    /**
     * Build WorkflowConfig from specified stream.  Variable expansion will be performed
     * against the specified BrigadeProperties instance.
     * @param in Stream to build from
     * @param bp BrigadeProperties instance to perform variable expansion against
     * @return A populated WorkflowConfig
     * @throws ConfigException if the WorkflowConfig could not be instantiated
     */
    static WorkflowConfig<StageConfig> fromStream(InputStream in, BrigadeProperties bp)
            throws ConfigException {
        return new EmptyWorkflowConfig().buildFromStream(in, Optional.of(bp));
    }
}
