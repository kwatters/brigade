package com.kmwllc.brigade.stage;

import com.google.common.base.Strings;
import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This stage will use a regex to find a pattern in a string field and store the
 * matched text into the output field.
 *
 * The list of keepGroups tells the RegexEtractor which groups from the regular
 * expression to keep. Groups are concatenated to form the output value.
 *
 * @author kwatters, dmeehl
 *
 */
public class RegexExtractor extends AbstractStage {

  private String inputField = null;
  private String outputField = null;
  private List<Integer> keepGroups = null;
  private String regex = null;
  private boolean multiFieldCapture = false;
  private boolean uniqueResults = false;

  private Pattern pattern;

  @Override
  public void startStage(StageConfig config) {
    if (config != null) {
      inputField = config.getProperty("inputField", "text");
      outputField = config.getProperty("outputField", "entity");
      List<String> keepGroupsStr = config.getListParam("keepGroups");
      regex = config.getProperty("regex");
      processOnlyNull = config.getBoolParam("processOnlyNull", processOnlyNull);
      multiFieldCapture = config.getBoolParam("multiFieldCapture", multiFieldCapture);
      uniqueResults = config.getBoolParam("uniqueResults", uniqueResults);

      keepGroups = new ArrayList<Integer>();
      if (keepGroupsStr == null) {
        keepGroups.add(1);
      } else {
        for (String groupNum : keepGroupsStr) {
          keepGroups.add(Integer.parseInt(groupNum));
        }
      }
    }
    pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
  }

  @Override
  public List<Document> processDocument(Document doc) {
    if (!doc.hasField(inputField)) {
      return null;
    }

    if (processOnlyNull && doc.hasField(outputField)) {
      return null;
    }

    Collection<String> matches = uniqueResults ? new HashSet<String>() : new ArrayList<String>();
    for (Object o : doc.getField(inputField)) {
      String text = o.toString();
      Matcher matcher = pattern.matcher(text);
      if (matcher.matches() && matcher.groupCount() > 0) {
        String match = "";
        for (Integer num : keepGroups) {
          if (multiFieldCapture) {
            String m = matcher.group(num);
            if (!Strings.isNullOrEmpty(m)) {
              matches.add(matcher.group(num));
            }
          } else {
            match += matcher.group(num);
          }
        }
        if (!multiFieldCapture) {
          matches.add(match);
        }
      }
    }

    doc.removeField(outputField);
    for (String match : matches) {
      doc.addToField(outputField, match);
    }

    // this stage doesn't emit child docs.
    return null;
  }

  @Override
  public void stopStage() {
  }

  @Override
  public void flush() {
  }

  public String getInputField() {
    return inputField;
  }

  public void setInputField(String inputField) {
    this.inputField = inputField;
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField(String outputField) {
    this.outputField = outputField;
  }

  public String getRegex() {
    return regex;
  }

  public void setRegex(String regex) {
    this.regex = regex;
  }

}
