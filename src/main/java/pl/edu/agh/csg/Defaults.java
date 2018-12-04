package pl.edu.agh.csg;

import java.util.HashMap;
import java.util.Map;

public class Defaults {
    public static Map<String, String> withDefault(Map<String, String> maybe) {
        if (maybe != null) {
            return maybe;
        }

        return new HashMap<String, String>();
    }

    public static String withDefault(String parameterName, String defaultValue) {
        String envVariableValue = System.getenv(parameterName);

        if (envVariableValue != null) {
            return envVariableValue;
        }

        return defaultValue;
    }
}
