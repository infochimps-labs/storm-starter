package com.infochimps.storm.spout.blob;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * 
 * Hint: Unchecked exceptions. Wrap in RuntimeException and throw. 
 * @author sa
 *
 */
public interface BlobStore extends Serializable {
    
    /**
     * Lazy initialization. Workaround for Storm as some objects might not be serializable. 
     * @return
     */
    public boolean initialize();
    
    /**
     * Get the next marker. 
     * @param currentMarker
     * @return nextMarker Return the next marker.If the current marker is the last one, return null.
     */
    public String getNextBlobMarker(String currentMarker);
    
    
    /**
     * Get a data stream to read the contents of a blob. The calling class has to close the stream properly.
     * @param blobMarker
     * @param context
     * @return
     */
    public InputStream getBlob(String blobMarker, Map<String, Object> context);
    
    /**
     * Get metadata for the marker.
     * @param blobMarker
     * @return jsonString - A json string containing the metadata.
     */
    public String getMetaData(String blobMarker);

}
