package com.infochimps.storm.spout.file;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import storm.trident.spout.ITridentSpout.BatchCoordinator;

public class FileBatchCoordintor implements BatchCoordinator<String> {

    private List<String> ids = new ArrayList<String>();
    private int counter = 0;

    public FileBatchCoordintor(String _dir) {
        System.out
                .printf("BatchCoordinator [%s] created using _dir : %s\n", this
                        .hashCode(), _dir);

        File folder = new File(_dir);
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile()) {
                System.out.println(file.getName());
                ids.add(file.getAbsolutePath());
            }
        }
    }

    private String next(String lastMetaDataId) {
        String tmp = "";
        if (counter == ids.size()) {
            return null;
        }
        tmp = ids.get(counter);
        System.out.println("next() : " + tmp);
        counter++;
        return tmp;
    }

    @Override
    public String initializeTransaction(long txid, String prevMetadata, String currMetadata) {
        System.out
                .printf("BatchCoordinator [%s].initializeTransaction() - txid :%s prev : %s new : %s\n", this
                        .hashCode(), txid, prevMetadata, currMetadata);

        if (currMetadata == null) {
            return next(currMetadata);
        }
        return currMetadata;

    }

    @Override
    public void success(long txid) {

    }

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {

    }

}
