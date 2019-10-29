package com.camel.kafka.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class CustomKafkaAvroSerializer extends AbstractKafkaAvroSerializer implements Serializer<Object> {

	private static final Logger LOG = LoggerFactory.getLogger(CustomKafkaAvroSerializer.class);
	private static final String SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081";
	private boolean useSpecificAvroReader = true;
	private boolean isKey;

	@Override
	public void close() {
	}

	@Override
	public byte[] serialize(String topic, Object record) {
		LOG.info("****************serialize*******************************");
		LOG.info("Serialize method: topic " + topic);
		LOG.info("Serialize method: byte " + record);
		Schema schema;
        int id;
        try {
            schema = ReflectData.get().getSchema(record.getClass());
            id = this.schemaRegistry.register(topic + "-value", schema);
//            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//			BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(byteArrayOutputStream, null);
//
//			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
//			datumWriter.write((GenericRecord) record, binaryEncoder);
//
//			binaryEncoder.flush();
//			byteArrayOutputStream.close();
//
//			byte[] data = byteArrayOutputStream.toByteArray();
//			System.out.println("Data is "+ new String(data, "ISO-8859-1"));
//			return data;
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
        return serializeImpl(id, schema, record);
    }

    protected byte[] serializeImpl(int id, Schema schema, Object object) throws SerializationException {
        if (object == null) {
            return null;
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(0x0);
            out.write(ByteBuffer.allocate(4).putInt(id).array());

            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            DatumWriter<Object> writer = new ReflectDatumWriter<Object>(schema);
            writer.write(object, encoder);
            encoder.flush();
            byte[] bytes = out.toByteArray();
            LOG.info("serialized bytes='{}'", DatatypeConverter.printHexBinary(bytes));
            out.close();
            return bytes;
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error serializing Avro message", e);
        }
	}

	@Override
	public void configure(KafkaAvroSerializerConfig config) {
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
		LOG.info("EXIT CustomKafkaAvroserializer  : configure method ");
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		configure(null);
	}
}
