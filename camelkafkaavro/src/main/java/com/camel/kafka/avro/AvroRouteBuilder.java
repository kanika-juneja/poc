package com.camel.kafka.avro;

import javax.xml.bind.JAXBContext;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.converter.jaxb.JaxbDataFormat;

import com.camel.kafka.avro.model.Employees;

public class AvroRouteBuilder extends RouteBuilder {

	@Override
	public void configure() throws Exception {

		// XML Data Format
		JaxbDataFormat xmlDataFormat = new JaxbDataFormat();
		JAXBContext con = JAXBContext.newInstance(Employees.class);
		xmlDataFormat.setContext(con);

		from("timer://foo?period={{period}}").setBody(constant("Hi")).process(new KafkaAvroMessageProcessor())
				.to("kafka:{{producer.topic}}" + "?brokers={{kafka.bootstrap.url}}");

		from("kafka:{{producer.topic}}"
				+ "?brokers={{kafka.bootstrap.url}}"
				//+ "&keySerializerClass=org.apache.kafka.common.serialization.StringSerializer&serializerClass=com.camel.kafka.avro.CustomKafkaAvroSerializer"
				).unmarshal(xmlDataFormat).process(new KafkaAvroMessageConsumerProcessor())
						.to("kafka:{{consumer.topic}}?brokers={{kafka.bootstrap.url}}");

	}
}
