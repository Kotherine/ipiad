package org.example;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class CrawlerConsumer {

    private static final String RESULT_QUEUE_NAME = "resultQueue";
    private static final Logger logger = LogManager.getLogger(CrawlerConsumer.class);

    // Основной метод, запускающий consumer
    public static void main(String[] args) throws Exception {
        // Настройка соединений RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kotherine");
        factory.setPassword("12345");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);

        // Создание соединения и канала RabbitMQ
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        // Объявление очереди результатов
        channel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);

        // Создание объекта для взаимодействия с Elasticsearch
        ElasticSearchUtil dbConnector = new ElasticSearchUtil(logger);

        // Создание consumer для чтения результатов из очереди
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                // Получение сообщения из очереди
                String message = new String(body, "UTF-8");
                String[] parts = message.split(";");
                String hash = parts[0];
                String url = parts[1];
                String title = parts[2];
                String publicationDate = parts[3];
                String tagsList = parts[4];

                // Вывод полученного сообщения
                System.out.println("News: " + String.format("\nTitle: %s\nPublication Date: %s\nTags: %s\nURL: %s", title, publicationDate, tagsList.toString(), url) + "\n");

                // Проверка существования документа в Elasticsearch
                News existingNews = dbConnector.readSingleDocument(hash);
                if (existingNews.getTitle().isEmpty()) {
                    // Документ не найден, сохраняем его
                    News news = new News(title, "", "", url, hash);
                    dbConnector.indexSingleDocument(hash, news);
                }

                // Подтверждение обработки сообщения
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        // Старт чтения результатов из очереди
        channel.basicConsume(RESULT_QUEUE_NAME, false, consumer);

        // Вывод сообщения о начале работы consumer
        System.out.println("Result consumer is waiting for messages. Press Ctrl+C to exit.");

        // Закрытие соединения с Elasticsearch при завершении программы
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                dbConnector.close();
                conn.close();
            } catch (IOException e) {
                logger.error("Error while closing resources: " + e.getMessage());
            }
        }));
    }
}