package com.kmwllc.brigade;

import com.kmwllc.brigade.util.BrigadeHelper;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by matt on 3/22/17.
 */
public class BrigadeBreakingTest {

  @Rule
  public final BrigadeHelper brigadeHelper2 = new BrigadeHelper("conf/brigade.properties",
          "conf/bad-connector.xml", "conf/vanilla-workflow.xml");

  @Rule
  public final BrigadeHelper brigadeHelper3 = new BrigadeHelper("conf/brigade.properties",
          "conf/connector.xml", "conf/bad-stage.xml");

  @Test
  public void testBadWorkflow() {
    try {
      BrigadeHelper brigadeHelper = new BrigadeHelper("conf/brigade.properties",
              "conf/connector.xml", "conf/bad-workflow.xml");
      brigadeHelper.exec();
    } catch (Exception e) {
      assertTrue(true);
      return;
    }
    // Shouldn't get to here
    fail();
  }

  @Test
  public void testBadConnector() {
    try {
      brigadeHelper2.exec();
    } catch (Exception e) {
      assertTrue(true);
      return;
    }
    // Shouldn't get to here
    fail();
  }

  @Test
  public void testBadStage() {
    try {
      brigadeHelper3.exec();
    } catch (Exception e) {
      assertTrue(true);
      return;
    }
    // Shouldn't get to here
    fail();
  }
}
