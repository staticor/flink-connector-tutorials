package org.pd.streaming.connector.kafka.examples;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class MyProducer<T>
{
	String bootstrapServers;
	KafkaProducer<String, T> producer;
	
	public MyProducer(String kafkaServer, String serializerName)
	{
		// 显示指定 kafkaServer 和 serializer 序列化(value的序列化)
		this.bootstrapServers = kafkaServer;

		//  Producer properties    生产者参数
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerName);

        // create the producer
        producer = new KafkaProducer<>(properties);
	}

	/**
	 *  向指定的topic 发送消息体
	 * @param topic  主题
	 * @param message 消息体
	 */
	public void send(String topic, T message)
	{
        // create a producer record
        ProducerRecord<String, T> record = new ProducerRecord<String, T>(topic, "myKey", message);

        // send data - asynchronous
        producer.send(record);
        
        // flush data
        producer.flush();
	}
	
	public void close()
	{
		// flush and close producer
        producer.close();
	}
}
