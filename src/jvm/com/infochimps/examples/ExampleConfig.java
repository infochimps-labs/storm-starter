package com.infochimps.examples;

import java.util.Enumeration;
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
    
    public static String getAll() {
    	
    	try {
    		StringBuilder sb = new StringBuilder();
    		Enumeration<String> keys = RESOURCE_BUNDLE.getKeys();
    		
    		while(keys.hasMoreElements()){
    			String tmp = keys.nextElement();
    			sb.append(tmp + "=" + RESOURCE_BUNDLE.getString(tmp) + " \n");
    		}
    		return sb.toString();
    	} catch (MissingResourceException e) {
    		return "!Nothing found";
    	}
    }
    
    
}
