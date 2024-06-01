package org.example;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Класс CrawlerConsumer предназначен для чтения и обработки сообщений из очереди результатов.
 */
public class CrawlerConsumer {

    // Название очереди результатов
    private static final String RESULT_QUEUE_NAME = "resultQueue";

    // Логгер для записи событий
    private static final Logger logger = LogManager.getLogger(CrawlerConsumer.class);

    /**
     * Точка входа в приложение.
     */
    public static void main(String[] args) {
        // Настройка соединения с RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("Kotherina");
        factory.setPassword("123");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);

        // Объявление переменных для соединения, канала и объекта для взаимодействия с Elasticsearch
        Connection conn = null;
        Channel channel = null;
        ElasticSearchUtil dbConnector = null;

        try {
            // Установка соединения и создание канала для чтения
            conn = factory.newConnection();
            channel = conn.createChannel();

            // Объявление очереди результатов
            channel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);

            // Инициализация объекта для взаимодействия с Elasticsearch
            dbConnector = new ElasticSearchUtil(logger);

            while (true) {
                // Получение сообщения из очереди результатов
                GetResponse response = channel.basicGet(RESULT_QUEUE_NAME, false);
                if (response == null) {
                    Thread.sleep(1000); // Если очередь пуста, ждем 1 секунду и повторяем
                    continue;
                }

                // Разбор сообщения
                String message = new String(response.getBody(), StandardCharsets.UTF_8);
                String[] parts = message.split(";", -1);
                String hash = parts[0];
                String url = parts[1];
                String title = parts[2];
                String publicationDate = parts[3];
                String category = parts[4];

                // Вывод информации о новости в консоль
                System.out.println("News: " + String.format("\nTitle: %s\nPublication Date: %s\nCategory: %s\nURL: %s", title, publicationDate, category, url));

                // Проверка существования новости в Elasticsearch
                News existingNews = dbConnector.readSingleDocument(hash);
                if (existingNews == null || existingNews.getTitle().isEmpty()) {
                    // Если новость не существует, индексируем ее в Elasticsearch
                    News news = new News(title, publicationDate, category, url, hash);
                    dbConnector.indexSingleDocument(hash, news);
                }

                // Подтверждение обработки сообщения
                channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
            }
        } catch (Exception e) {
            logger.error("Error in consumer: " + e.getMessage(), e);
        } finally {
            // Закрытие ресурсов в блоке finally для обеспечения их закрытия при завершении работы программы
            try {
                if (dbConnector != null) {
                    dbConnector.close();
                }
                if (channel != null) {
                    channel.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (IOException | TimeoutException e) {
                logger.error("Error closing resources: " + e.getMessage(), e);
            }
        }
    }
}
