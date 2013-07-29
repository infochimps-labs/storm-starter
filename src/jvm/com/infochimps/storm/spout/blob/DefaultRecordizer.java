package com.infochimps.storm.spout.blob;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import javax.management.RuntimeErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class DefaultRecordizer implements IRecordizer{
    private static final String CHARACTER_SET = "UTF-8";
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRecordizer.class);


    @Override
    public boolean recordize(InputStream blobData, TridentCollector collector, Map<String, Object>context) {
        String line;
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(blobData, CHARACTER_SET));
            while ((line = reader.readLine()) != null) {
                collector.emit(new Values(line));
                LOG.trace(Utils.logString("emitPartitionBatch", context.get("compId").toString(), context.get("txId").toString(),"emitted", line));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
            
        }finally {
            if (reader != null)
                try { reader.close();} catch (IOException e) {
                    LOG.error(Utils.logString("emitPartitionBatch", context.get("compId").toString(), context.get("txId").toString(),"Error in closing the data stream."), e);
                }
        }
        return false;
    }

}
