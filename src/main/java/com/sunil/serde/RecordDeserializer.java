package com.sunil.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sunil.objects.Record;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Record deserializer class for Kafka messages
 * */
public class RecordDeserializer implements Deserializer<Record> {
    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {}

    @Override
    public Record deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        Record record = null;
        try {
            System.out.println("arg1: " + new String(arg1) + "arg0:  " + arg0);
            record = mapper.readValue(new String(arg1), Record.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return record;
    }
}
