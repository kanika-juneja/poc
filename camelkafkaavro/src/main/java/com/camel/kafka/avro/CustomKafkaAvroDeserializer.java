package com.camel.kafka.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class CustomKafkaAvroDeserializer extends AbstractKafkaAvroDeserializer implements Deserializer<Object> {

	private static final Logger LOG = LoggerFactory.getLogger(CustomKafkaAvroDeserializer.class);
	private static final String SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081";

	@Override
	public void configure(KafkaAvroDeserializerConfig config) {
		LOG.info("ENTER CustomKafkaAvroDeserializer  : configure method ");
		LOG.info("ENTER CustomKafkaAvroDeserializer  : SCHEMA_REGISTRY_URL " + SCHEMA_REGISTRY_URL);

		if (SCHEMA_REGISTRY_URL == null) {
			throw new org.apache.kafka.common.config.ConfigException("No schema registry provided");
		}
		try {

			final List<String> schemas = Collections.singletonList(SCHEMA_REGISTRY_URL);
			this.schemaRegistry = new CachedSchemaRegistryClient(schemas, Integer.MAX_VALUE);
			this.useSpecificAvroReader = true;

		} catch (ConfigException e) {
			e.printStackTrace();
			throw new org.apache.kafka.common.config.ConfigException(e.getMessage());
		}
		LOG.info("EXIT CustomKafkaAvroDeserializer  : configure method ");

	}

	@SuppressWarnings("unchecked")
	@Override
	public Object deserialize(String topic, byte[] data) {
		try {
			Object result = null;
			ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
			if (data != null) {
				LOG.info("serialized bytes='{}'", DatatypeConverter.printHexBinary(data));
				
				Object emp = getObject(data);

				Parser parser = new Parser();
				// Schema schema = null;
				// try {
				// schema = parser.parse(CustomKafkaAvroDeserializer.class.getClassLoader()
				// .getResourceAsStream("avro/employee.avsc"));
				// } catch (IOException e1) {
				// TODO Auto-generated catch block
				// e1.printStackTrace();
				// }
				Schema schema = ReflectData.get().getSchema(Employee.class);
				ReflectDatumReader<Object> reader = new ReflectDatumReader<>(schema);
				Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
				result = reader.read(null, decoder);
				// DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
				// Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

				// result = (Object)datumReader.read(null, decoder);
				// LOG.debug("deserialized data='{}'", result);
			}
			return (Object) result;

		} catch (Exception ex) {
			throw new SerializationException(
					"Can't deserialize data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
		}

	}

	/* De serialize the byte array to object */
	private static Object getObject(byte[] byteArr) throws IOException, ClassNotFoundException {
		Object object = SerializationUtils.deserialize(byteArr);
		ByteArrayInputStream bis = new ByteArrayInputStream(byteArr);
		ObjectInput in = new ObjectInputStream(bis);
		//return in.readObject();
		return object;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		configure(null);
	}

	@Override
	public void close() {
	}
}
