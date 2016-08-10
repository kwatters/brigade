package com.kmwllc.brigade.stage;

import org.slf4j.Logger;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This stage will divide the values of two fields a put the resulting quotient in the quotentField.
 * Use the "MathValues" stage instead that's based on exp4j.  
 * 
 * @author kwatters
 *
 */
@Deprecated
public class DivideValues extends AbstractStage {

  // TODO: remove this .. maybe we should depricate?
  public final static Logger log = LoggerFactory.getLogger(DivideValues.class.getCanonicalName());

  private String dividendField = null;
  private String divisorField = null;
  private String quotentField = null;

  @Override
  public void startStage(StageConfig config) {

    if (config != null) {
      dividendField = config.getProperty("dividendField");
      divisorField = config.getProperty("divisorField");
      quotentField = config.getProperty("quotentField");
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    // divide the double values in 2 fields, store the result in the quotent
    // field.
    if (!(doc.hasField(dividendField) && doc.hasField(divisorField))) {
      return null;
    }
    if (doc.getField(dividendField).size() != doc.getField(divisorField).size()) {
      log.warn("Dividend and Divisor fields of unequal length.");
      return null;
    }
    ArrayList<Double> results = new ArrayList<Double>();
    int size = doc.getField(dividendField).size();
    for (int i = 0; i < size; i++) {
      try {
        // log.info("Compute {} divided by {}",
        // doc.getField(dividendField).get(i),
        // doc.getField(divisorField).get(i));
        Double divisor = convertToDouble(doc.getField(divisorField).get(i));
        Double dividend = convertToDouble(doc.getField(dividendField).get(i));
        if (divisor == 0.0) {
          continue;
        }
        Double quotient = dividend / divisor;
        results.add(quotient);
      } catch (ClassCastException e) {
        log.warn("Division Error DocID: ", doc.getId());
        e.printStackTrace();
      }
    }

    if (dividendField.equals(quotentField)) {
      doc.removeField(quotentField);
    }
    for (Double v : results) {
      doc.addToField(quotentField, v);
    }

    return null;
  }

  private Double convertToDouble(Object obj) throws ClassCastException {
    Double doubleVal = null;
    if (obj instanceof Integer) {
      doubleVal = new Double(((Integer) obj).intValue());
    } else if (obj instanceof Double) {
      doubleVal = (Double) obj;
    } else {
      throw new ClassCastException("Cannot convert " + obj.getClass().getName() + " to Double.");
    }

    return doubleVal;
  }

  @Override
  public void stopStage() {
    // no op for this stage
  }

  @Override
  public void flush() {
    // no-op for this stage

  }

}
