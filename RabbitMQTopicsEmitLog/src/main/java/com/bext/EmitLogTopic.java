package com.bext;


import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLogTopic {
	private static final String EXCHANGE_NAME = "topic_logs";
	
	public static void main(String[] args) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		try (Connection connection = factory.newConnection();
			 Channel channel = connection.createChannel()) {
			
			channel.exchangeDeclare(EXCHANGE_NAME, "topic");
			
			String routingKey = getRoutingKey(args);
			String message = getMessage(args);
			
			channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
			System.out.println(" [x] Enviado '" + routingKey + "':'" + message + "'");
		};
	}

	private static String getMessage(String[] args) {
		return args[1];
	}

	private static String getRoutingKey(String[] args) {
		return args[0];
	}

}
