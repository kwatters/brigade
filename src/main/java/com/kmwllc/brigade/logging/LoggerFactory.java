package com.kmwllc.brigade.logging;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

/**
 * our own logger factory to create a logger for classed in the framework.
 * 
 * @author kwatters
 *
 */
public class LoggerFactory {

  public static Logger getLogger(Class<?> clazz) {
    return getLogger(clazz.toString());
  }

  public static Logger getLogger(String name) {
    return org.slf4j.LoggerFactory.getLogger(name);
  }

  public static ILoggerFactory getILoggerFactory() {
    return org.slf4j.LoggerFactory.getILoggerFactory();
  }

}
