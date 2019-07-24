package com.bext;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogsTopic {
	private static final String EXCHANGE_NAME = "topic_logs";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		if (args.length < 1) {
			System.err.println("Uso: ReceiveLogsTopic [binding key]...");
			System.exit(1);
		}
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
		String queueName = channel.queueDeclare().getQueue();
		
		for (String bindingKey : args) {
			channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
		}
		System.out.println("[x] Esperando por mensajes. CTRL+C para salir.");
		
		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");
			System.out.println("[x] recibido '" + 
					delivery.getEnvelope().getRoutingKey() +
					"':'" + message + "'");
		};
		boolean autoack = true;
		channel.basicConsume(queueName, autoack, deliverCallback, consumerTag->{});
	}
}
