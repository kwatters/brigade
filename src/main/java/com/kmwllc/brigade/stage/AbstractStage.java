package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * This is the base class for all stages in a pipeline.  The stages have a lifecycle
 * startStage takes the configuration and initalizes.
 * process document is where the main processing of a document happens,
 * flush tells the stage to flush any pending data
 * stopStage is (should be) called when the stage is shutting down.
 * <p>
 * Additionally, stages have access to Brigade properties; these are passed to the stage during
 * its initialization.  It should be noted that these properties cannot be changed once the pipeline
 * has started (ie BrigadeRunner.exec has been called)
 * <p>
 * There are two fields which can be set on a stage to determine whether it should run based upon
 * current conditions.<ul>
 *   <li>enabled - If set, the stage will only run if the property pointed to by 'enabled' is true </li>
 *   <li>skipIfField - If set, the stage will be skipped if the field is present in the document and is true</li>
 * </ul>
 * A stage may set its stageExceptionMode.  By default, a stage inherits its stageExceptionMode from the
 * pipeline.  Overriding this at a stage level allows for fine-grained customization of behavior within a
 * pipeline
 * @author kwatters
 */
public abstract class AbstractStage implements Stage {

  private Map<String, String> props = new HashMap<>();
  private String enabled;
  private String skipIfField;
  private String name;
  private StageExceptionMode stageExceptionMode;

  // TODO: make sure the subclasses get the right logger..
  public final static Logger log = LoggerFactory.getLogger(AbstractStage.class.getCanonicalName());
  // Process only when output field doesn't exist in the document
  // Stages that support this should check and handle it in their
  // processDocument()
  protected boolean processOnlyNull = false;

  public void init(StageConfig config) {
    enabled = config.getStringParam("enabled");
    skipIfField = config.getStringParam("skipIfField");
    String seModeS = config.getStageExceptionModeClass();
    if (seModeS != null) {
      try {
        stageExceptionMode = StageExceptionMode.valueOf(seModeS);
      } catch (IllegalArgumentException e) {
        log.warn("Unknown StageExceptionMode:  {}.  Will default to NEXT_DOC", seModeS);
        stageExceptionMode = StageExceptionMode.NEXT_DOC;
      }
    }
    startStage(config);
  }

  public abstract void startStage(StageConfig config);

  public abstract void stopStage();

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public String getEnabled() {
    return enabled;
  }

  @Override
  public String getSkipIfField() {
    return skipIfField;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public StageExceptionMode getStageExceptionMode() {
    return stageExceptionMode;
  }

  public void setStageExceptionMode(StageExceptionMode stageExceptionMode) {
    this.stageExceptionMode = stageExceptionMode;
  }
}
