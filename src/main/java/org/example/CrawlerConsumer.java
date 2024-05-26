package org.example;

import com.rabbitmq.client.*;

import java.io.IOException;

public class CrawlerConsumer {

    private static final String RESULT_QUEUE_NAME = "resultQueue";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kotherine");
        factory.setPassword("12345");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        channel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received: " + message + "\n");

                // Подтверждаем сообщение
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(RESULT_QUEUE_NAME, false, consumer);

        System.out.println("Result consumer is waiting for messages. Press Ctrl+C to exit.");
    }
}
