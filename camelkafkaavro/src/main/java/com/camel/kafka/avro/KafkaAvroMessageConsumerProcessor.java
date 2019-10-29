package com.camel.kafka.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.camel.kafka.avro.model.Employee;
import com.camel.kafka.avro.model.Employees;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

public class KafkaAvroMessageConsumerProcessor implements Processor {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroMessageConsumerProcessor.class);

	private static final String SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081";

	@SuppressWarnings("deprecation")
	@Override
	public void process(Exchange exchange) throws Exception {
		Employees object = exchange.getIn().getBody(Employees.class);
		LOG.info("KafkaAvroMessageConsumerProcessor:" + object);
		// associated topic name.
		String topic = "avrotopicnew9";
		// avro schema avsc file path.
		String schemaPath = "avro/employee.avsc";
		// subject convention is "<topic-name>-value"
		String subject = topic + "-value";

		Parser parser = new Parser();
		// avsc json string.
		Schema schema = null;
		// schema = ReflectData.get().getSchema(object.getClass());
		try {
			schema = parser.parse(CustomKafkaAvroDeserializer.class.getClassLoader().getResourceAsStream(schemaPath));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, Integer.MAX_VALUE);
		int id = client.register(subject, schema);
		List<byte[]> bytes = serialize(id, schema, object.getEmployees());
		exchange.getOut().setBody(bytes);
	}

	private List<byte[]> serialize(int id, Schema schema, List<Employee> employees) throws IOException {

		if (employees == null) {
			return null;
		}
		ByteArrayOutputStream out = null;
		List<byte[]> bytesList = new ArrayList<>();
		byte[] bytes = null;
		DatumWriter<Object> writer = new ReflectDatumWriter<>(schema);
		try {
			for (Employee emp : employees) {
				out = new ByteArrayOutputStream();
				out.write(0x0);
				out.write(ByteBuffer.allocate(4).putInt(id).array());
				BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
				writer.write(emp, encoder);
				encoder.flush();
				bytes = out.toByteArray();
				bytesList.addAll(Arrays.asList(bytes));
				LOG.info("serialized bytes='{}'", DatatypeConverter.printHexBinary(bytes));
			}
		} catch (IOException | RuntimeException e) {
			throw new SerializationException("Error serializing Avro message", e);
		} finally {
			out.close();
		}
		return bytesList;
	}
}
