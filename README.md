# poc
Camel-Kafka-Avro-Xml

The sample employee xml string is pushed in one kafka topic. The data is consumed from this topic, converted into avro format and pushed into the other topic.

The route builder unmarshalls the xml string into java object.
