package com.camel.kafka.avro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@ImportResource({"classpath:spring/camel-context.xml"})
public class CamelKafkaAvroApplication {

	public static void main(String[] args) {
		SpringApplication.run(CamelKafkaAvroApplication.class, args);
	}

}
