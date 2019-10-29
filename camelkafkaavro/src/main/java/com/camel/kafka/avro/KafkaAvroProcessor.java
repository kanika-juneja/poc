package com.camel.kafka.avro;

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class KafkaAvroProcessor implements Processor{

private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroProcessor.class);
    
    @Override
    public void process(Exchange exc) throws Exception {
        @SuppressWarnings("unchecked")
        List<RecordMetadata> recordMetaData1 = (List<RecordMetadata>) exc.getIn().getHeader(KafkaConstants.KAFKA_RECORDMETA);
        for (RecordMetadata rd: recordMetaData1) {
            LOG.info("producer partition is:"  + rd.partition());
            LOG.info("producer partition message is:"  + rd.toString());
        }
    }
}
