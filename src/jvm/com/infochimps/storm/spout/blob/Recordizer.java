package com.infochimps.storm.spout.blob;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;

import storm.trident.operation.TridentCollector;

public interface Recordizer extends Serializable {

    public boolean recordize(InputStream blobData, TridentCollector collector, Map<String, Object>context);
}
