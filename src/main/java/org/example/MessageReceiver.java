package org.example;

import com.rabbitmq.client.*;

public class MessageReceiver {

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kotherine");
        factory.setPassword("12345");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        String exchangeName = "myExchange";
        String queueName = "myQueue";
        String routingKey = "testRoute";
        boolean durable = true;
        channel.exchangeDeclare(exchangeName, "direct", durable);
        channel.queueDeclare(queueName, durable, false, false, null);
        channel.queueBind(queueName, exchangeName, routingKey);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                new MessageThread(channel, new String(body), envelope.getDeliveryTag()).start();
            }
        };
        channel.basicConsume(queueName, false, consumer);

        // Keep the program running to listen for messages
        System.out.println("Waiting for messages. Press Ctrl+C to exit.");
        Thread.currentThread().join();
    }

}
