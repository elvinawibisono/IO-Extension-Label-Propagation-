package com.castsoftware.exporter.config;

import com.castsoftware.exporter.exceptions.ExporterException;
import com.castsoftware.exporter.exceptions.file.FileCorruptedException;
import com.castsoftware.exporter.exceptions.file.FileIOException;

import java.io.IOException;
import java.util.Properties;

public class getConfigValues {


    public static final String CONFIGURATION_PATH = "/config.properties";

    public enum Property {
        NO_RELATIONSHIP_WEIGHT("no_relationship_weight"),
        NO_RELATIONSHIP("no_relationship"),
        NODE_PROP_TYPE("node_property_type"),
        RELATIONSHIP_PROP_TYPE("relationship_prop_type"),
        RELATIONSHP_PROP_VALUE("relationship_prop_value"),
        NODE_LABELS("node_labels"); 

        private final String label;

        @Override
        public String toString() {
            try {
                return loadFromPropertyFile(this.label);
            } catch (ExporterException e) {
                e.printStackTrace();
                return null;
            }
        }

        Property(String label) {
            this.label = label;
        }
    }

    private static Properties properties;

    //Config node name
    public static  String loadFromPropertyFile(String key) throws FileIOException, FileCorruptedException {
        if (properties == null) {
            properties = new Properties();
            try {
                properties.load(getConfigValues.class.getResourceAsStream(CONFIGURATION_PATH));
            } catch (IOException | NullPointerException e) {
                throw new FileIOException("Cannot read config.properties file", CONFIGURATION_PATH, e, "CONFxLPR1");
            } catch (IllegalArgumentException e) {
                throw new FileCorruptedException("Corrupted config.properties file", CONFIGURATION_PATH, e, "CONFxLPR2");
            }
        }
        try {
            return  (String) properties.get(key);
        } catch (NullPointerException e) {
            throw new FileCorruptedException("Cannot read properties in config.properties file", CONFIGURATION_PATH, e, "CONFxLPR1");
        }
    }

    public getConfigValues() {
    }
    
    
}
