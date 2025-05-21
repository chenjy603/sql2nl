package com.code.utils;

public class StringUtils {
    public static String snakeToCamel(String input) {
        String[] components = input.split("_");
        StringBuilder camelCaseString = new StringBuilder();
        boolean capitalizeNext = false;

        for (String component : components) {
            if (component.isEmpty()) continue;

            if (capitalizeNext) {
                camelCaseString
                        .append(component.substring(0, 1).toUpperCase())
                        .append(component.substring(1));
                capitalizeNext = false;
            } else {
                camelCaseString.append(component);
            }

            if (!component.equals(components[components.length - 1])) {
                capitalizeNext = true;
            }
        }

        return camelCaseString.toString();
    }
}
