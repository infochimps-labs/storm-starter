package com.infochimps.storm.spout.blob.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.infochimps.storm.spout.blob.BlobStore;

public class FileBlobStore implements BlobStore{

    private List<String> ids = new ArrayList<String>();
    private int counter = 0;
    private String _dir;
    
    
    
    public FileBlobStore(String _dir) {
        this._dir = _dir;
    }

    @Override
    public boolean initialize() {
        File folder = new File(_dir);
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile()) {
                //System.out.println(file.getName());
                ids.add(file.getAbsolutePath());
            }
        }        
        
        return true;
    }

    @Override
    public String getNextBlobMarker(String currentMarker) {
        String tmp = "";
        if (counter == ids.size()) {
            return null;
        }
        tmp = ids.get(counter);
        //System.out.println("next() : " + tmp);
        counter++;
        return tmp;
    }

    @Override
    public InputStream getBlob(String blobMarker, Map<String, Object> context) {
        FileInputStream br;
            try {
                br = new FileInputStream(blobMarker);
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            }
        return br;
    }
    

}
