package com.infochimps.examples;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class ExampleConfig {
    private static final String BUNDLE_NAME = "config"; //$NON-NLS-1$

    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle
            .getBundle(BUNDLE_NAME);

    private ExampleConfig() {
    }

    public static String getString(String key) {
        try {
            return RESOURCE_BUNDLE.getString(key);
        } catch (MissingResourceException e) {
            return '!' + key + '!';
        }
    }
}
