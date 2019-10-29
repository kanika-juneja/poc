package com.camel.kafka.avro;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class KafkaAvroMessageProcessor implements Processor {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroProcessor.class);

	@SuppressWarnings("deprecation")
	public void process(Exchange exc) throws Exception {
		//Employee emp = Employee.newBuilder().setFirstName("kanika").setLastName("juneja").build();
		String emp = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><employees><employee><firstName>Katy</firstName><lastName>Perry</lastName></employee><employee><firstName>John</firstName><lastName>Wick</lastName></employee></employees>";
		//String emp = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><employee><firstName>Katy</firstName><lastName>Perry</lastName></employee>";
		exc.getMessage().setBody(emp);
	}
}
