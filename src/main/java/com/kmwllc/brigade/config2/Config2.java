package com.kmwllc.brigade.config2;

import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Map;

public interface Config2<T extends Config2> {

    Map<String, Object> getConfig();

    default void setStringParam(String name, String value) {
        getConfig().put(name, value);
    }

    default String getStringParam(String name) {
        return getStringParam(name, null);
    }

    default String getStringParam(String name, String defaultValue) {
        if (getConfig().containsKey(name)) {
            Object val = getConfig().get(name);
            if (val instanceof String) {
                return ((String) val).trim();
            } else {
                return val.toString().trim();
            }
        } else {
            return defaultValue;
        }
    }

    default String getProperty(String name) {
        return getStringParam(name);
    }

    default String getProperty(String name, String defaultValue) {
        String val = getStringParam(name);
        if (val == null) {
            return defaultValue;
        } else {
            return val;
        }
    }

    default void setIntegerParam(String name, Integer value) {
        getConfig().put(name, value);
    }

    default Integer getIntegerParam(String name, Integer defaultValue) {
        if (getConfig().containsKey(name)) {
            Object val = getConfig().get(name);
            if (val instanceof Integer) {
                return (Integer) val;
            } else {
                return Integer.valueOf(val.toString());
            }
        } else {
            return defaultValue;
        }
    }

    default void setBoolParam(String name, Boolean value) {
        getConfig().put(name, value);
    }

    default Boolean getBoolParam(String name, Boolean defaultValue) {
        if (getConfig().containsKey(name)) {
            Object val = getConfig().get(name);
            if (val instanceof Boolean) {
                return (Boolean) val;
            } else {
                return Boolean.valueOf(val.toString());
            }
        } else {
            return defaultValue;
        }
    }

    default void setStringArrayParam(String name, String[] values) {
        getConfig().put(name, values);
    }

    default String[] getStringArrayParam(String name) {
        if (getConfig().containsKey(name)) {
            Object val = getConfig().get(name);
            if (val instanceof String[]) {
                return (String[]) val;
            } else {
            }
        }
        String[] empty = new String[0];
        return empty;
    }

    default void setListParam(String name, List<String> values) {
        getConfig().put(name, values);
    }

    default List<String> getListParam(String name) {
        Object val = getConfig().get(name);
        if (val instanceof List) {
            return (List<String>) val;
        }
        return null;
    }

    default Map<String, String> getMapParam(String name) {
        // TODO type safety?!
        return (Map<String, String>) getConfig().get(name);
    }

    default void setMapParam(String name, Map<String, String> map) {
        getConfig().put(name, map);
    }

    default void setObjectParam(String name, Object value) {
        getConfig().put(name, value);
    }

    default Object getObjectParam(String name) {
        return getConfig().get(name);
    }

    void serialize(Writer w) throws ConfigException;
    T deserialize(Reader r) throws ConfigException;
}
