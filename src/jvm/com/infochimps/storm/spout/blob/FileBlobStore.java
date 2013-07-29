package com.infochimps.storm.spout.blob;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;


public class FileBlobStore implements IBlobStore{

    private List<String> ids = new ArrayList<String>();
    private int counter = 0;
    private String _dir;
    private String lastMarker;
    
    
    
    public FileBlobStore(String _dir) {
        this._dir = _dir;
    }

    @Override
    public boolean initialize() {
        File folder = new File(_dir);
        if(!folder.exists()) throw new RuntimeException(_dir + " doesnot exist.");
        
        
        Collection<File> listOfFiles = FileUtils.listFiles(folder, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        if(listOfFiles.size() == 0) throw new RuntimeException(_dir + " doesnot have any files.");
        
        for (File file : listOfFiles) {
            if (file.isFile()) {
                //System.out.println(file.getName());
                ids.add(file.getAbsolutePath());
            }
        }        
        System.out.println(ids);
        lastMarker = ids.get(ids.size() - 1);
        return true;
    }

    @Override
    public String getNextBlobMarker(String currentMarker) {
        
        
        // Check if we have already read to the end of the list -> O(1)
        if (currentMarker.equals(lastMarker)) return null;
        
        // if marker=BEGINNING start from the beginning.
        if(currentMarker.equals(IBlobStore.START_FROM_BEGINNING)) return ids.get(0);
        
        // Get position of currentMarker
        int currentPosition = ids.indexOf(currentMarker);
        if(currentPosition == -1) throw new RuntimeException(currentMarker + " not found in directory: " + _dir +" ."); 
        
        // Return nextMarker
        return ids.get(currentPosition + 1);
    }

    @Override
    public InputStream getBlob(String blobMarker, Map<String, Object> context) {
        
        // Get position of currentMarker
        if(ids.indexOf(blobMarker) == -1) throw new RuntimeException(blobMarker + " not found in directory: " + _dir +" ."); 
        
        FileInputStream br;
            try {
                br = new FileInputStream(blobMarker);
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            }
        return br;
    }

    @Override
    public String getMetaData(String blobMarker) {
        return "{ \"fileName\" : \"" + blobMarker + "\"}";
    }
    

}
