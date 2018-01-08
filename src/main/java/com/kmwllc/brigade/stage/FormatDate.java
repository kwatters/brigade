package com.kmwllc.brigade.stage;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;
import org.slf4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This stage will take the values in the inputField and attempt to parse them
 * into a date object based on the formatString. The successfully parsed values
 * will be stored in the outputField. The values will overwrite the outputField
 * values.
 * 
 * @author kwatters
 *
 */
public class FormatDate extends AbstractStage {

  public final static Logger log = LoggerFactory.getLogger(FormatDate.class.getCanonicalName());

  private String inputField = "date";
  private String outputField = "date_string";
  private String inputFormatString = "yyymmdd";
  private String outputFormatString = "yyymmdd";
  private SimpleDateFormat sdf = null;

  @Override
  public void startStage(StageConfig config) {
    if (config != null) {
      inputField = config.getProperty("inputField");
      outputField = config.getProperty("outputField", "date");
      inputFormatString = config.getStringParam("inputFormatString", inputFormatString);
      outputFormatString = config.getStringParam("outputFormatString", outputFormatString);
    }
    // compile the date string parsers.
    sdf = new SimpleDateFormat(outputFormatString);
  }

  @Override
  public List<Document> processDocument(Document doc) {
    if (!doc.hasField(inputField)) {
      return null;
    }
    ArrayList<String> formattedDates = new ArrayList<String>();
    for (Object val : doc.getField(inputField)) {
      if (val instanceof String) {
    	  try {
			Date date = new SimpleDateFormat(inputFormatString).parse(val.toString().replaceAll("Z$", "+0000"));
			String formattedDate = sdf.format(date);
	        formattedDates.add(formattedDate);
	        break; // found one date, no need for more (avoid double date in some fields)
		} catch (ParseException e) {
			log.warn(val.toString() + " could not be properly parsed: " + e);
		}
      }
      if (val instanceof Date) {
        String formattedDate = sdf.format(val);
        formattedDates.add(formattedDate);
      }
    }
    // TODO: configure input/output overwrite vs append mode.
    if (inputField.equals(outputField)) {
      doc.removeField(outputField);
    }
    for (String d : formattedDates) {
      doc.addToField(outputField, d);
    }
    return null;
  }

  @Override
  public void stopStage() {
    // NOOP
  }

  @Override
  public void flush() {
    // NOOP
  }

}
