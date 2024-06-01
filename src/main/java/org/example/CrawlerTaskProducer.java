package org.example;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Производитель задач для веб-сканера.
 */
public class CrawlerTaskProducer {

    // Название очереди задач
    private static final String TASK_QUEUE_NAME = "taskQueue";

    /**
     * Точка входа в приложение.
     */
    public static void main(String[] args) throws Exception {
        // Создание фабрики соединений RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("Kotherina");
        factory.setPassword("123");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);

        // Установка соединения с RabbitMQ и создание канала
        try (Connection conn = factory.newConnection(); Channel channel = conn.createChannel()) {
            // Объявление очереди задач
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

            // Получение RSS-ленты и выбор всех элементов "item"
            Document rssFeed = Jsoup.connect("https://sosenskoe-omsu.ru/news/feed/").get();
            Elements items = rssFeed.select("item");

            // Итерация по каждому элементу "item"
            for (Element item : items) {
                // Извлечение ссылки, заголовка и хэша
                String link = item.select("link").text();
                String title = item.select("title").text();
                String hash = computeHash(link + title);

                // Формирование сообщения для очереди задач
                String message = String.format("%s;%s;%s", hash, link, title);

                // Отправка сообщения в очередь задач
                channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                System.out.println("Sent '" + message + "'");
            }
        }
    }

    /**
     * Вычисляет хэш строки с использованием алгоритма MD5.
     *
     * @param text Исходная строка
     * @return Хэш строки
     */
    private static String computeHash(String text) {
        try {
            // Создание объекта MessageDigest с использованием алгоритма MD5
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] array = md.digest(text.getBytes());
            StringBuilder sb = new StringBuilder();

            // Преобразование массива байт в строку шестнадцатеричного представления
            for (byte b : array) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // В случае ошибки NoSuchAlgorithmException выбрасывается RuntimeException
            throw new RuntimeException(e);
        }
    }
}

