package org.pd.streaming.connector.kafka.examples;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@SuppressWarnings("serial")
public class MySchema implements KafkaDeserializationSchema<SimpleKafkaRecord>
{
	@Override
	public boolean isEndOfStream(SimpleKafkaRecord nextElement) {
		return false;
	}

	@Override
	public SimpleKafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		SimpleKafkaRecord data = new SimpleKafkaRecord();
		data.key =  new String(record.key());
		data.value = new String(record.value());
		data.timestamp = record.timestamp();
		
		return data;
	}

	@Override
	public TypeInformation<SimpleKafkaRecord> getProducedType() {

		return TypeInformation.of(SimpleKafkaRecord.class);
	}
}



