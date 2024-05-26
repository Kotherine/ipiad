package org.example;

import com.rabbitmq.client.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class CrawlerConsumer {

    private static final String RESULT_QUEUE_NAME = "resultQueue";
    private final RestHighLevelClient elasticsearchClient;
    public CrawlerConsumer(RestHighLevelClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

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

        RestHighLevelClient elasticsearchClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        CrawlerConsumer crawlerConsumer = new CrawlerConsumer(elasticsearchClient);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received: " + message + "\n");
                String[] parts = message.split(";");
                String hash = parts[0];
                String url = parts[1];
                String title = parts[2];

                if (!ElasticSearchUtil.documentExists(elasticsearchClient, hash)) {
                    ElasticSearchUtil.saveDocument(elasticsearchClient, hash, title, "", message, url, "Unknown Author");
                }
                // Подтверждаем сообщение
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(RESULT_QUEUE_NAME, false, consumer);

        System.out.println("Result consumer is waiting for messages. Press Ctrl+C to exit.");
    }
}
