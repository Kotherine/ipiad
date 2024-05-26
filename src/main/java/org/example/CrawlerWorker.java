package org.example;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CrawlerWorker {

    private static final String TASK_QUEUE_NAME = "taskQueue";
    private static final String RESULT_QUEUE_NAME = "resultQueue";
    private static final HashSet<String> seenHashes = new HashSet<>();
    private static final Logger logger = LogManager.getLogger(CrawlerWorker.class);

    // Основной метод, запускающий worker
    public static void main(String[] args) throws Exception {
        // Настройка соединений RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("kotherine");
        factory.setPassword("12345");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);

        // Создание соединения и каналов RabbitMQ
        Connection conn = factory.newConnection();
        Channel taskChannel = conn.createChannel();
        Channel resultChannel = conn.createChannel();

        // Объявление очередей задач и результатов
        taskChannel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        resultChannel.queueDeclare(RESULT_QUEUE_NAME, true, false, false, null);

        // Создание объекта для взаимодействия с Elasticsearch
        ElasticSearchUtil dbConnector = new ElasticSearchUtil(logger);

        // Создание пула потоков для worker
        ExecutorService executor = Executors.newFixedThreadPool(10);

        // Создание consumer для чтения задач из очереди задач
        Consumer consumer = new DefaultConsumer(taskChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                // Получение сообщения из очереди
                String message = new String(body, "UTF-8");
                String[] parts = message.split(";");
                String hash = parts[0];
                String url = parts[1];
                String title = parts[2];

                // Проверка, обработано ли уже сообщение с таким хэшем
                if (seenHashes.contains(hash)) {
                    taskChannel.basicAck(envelope.getDeliveryTag(), false);
                    return;
                }
                seenHashes.add(hash);

                // Выполнение задачи в отдельном потоке
                executor.submit(() -> {
                    try {
                        // Скачивание и парсинг страницы
                        Document doc = Jsoup.connect(url).get();
                        Element pubDate = doc.selectFirst("date_rt_news");
                        String publicationDate = "";
                        if (pubDate != null) {
                            publicationDate = pubDate.text();
                        } else {
                            Elements pubDates = doc.getElementsByClass("date_rt_news");
                            for (Element date : pubDates) {
                                publicationDate = date.text();
                            }
                        }

                        Element newsTagsDiv = doc.selectFirst("div.news_tags");
                        StringBuilder tagsList = new StringBuilder();
                        if (newsTagsDiv != null) {
                            Elements tags = newsTagsDiv.select("a");
                            for (Element tag : tags) {
                                if (tagsList.length() > 0) {
                                    tagsList.append(", ");
                                }
                                tagsList.append(tag.text());
                            }
                        }
// Создание объекта News
                        News news = new News(title, publicationDate, tagsList.toString(), url, hash);

                        // Индексирование документа в Elasticsearch
                        dbConnector.indexSingleDocument(hash, news);

                        // Формирование строки результата
                        String result = String.format("%s;%s;%s;%s;%s", hash, url, title, publicationDate, tagsList.toString());

                        // Отправка результата в очередь результатов
                        resultChannel.basicPublish("", RESULT_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, result.getBytes("UTF-8"));
                    } catch (IOException e) {
                        logger.error("Error processing message: " + e.getMessage(), e);
                    }
                });
            }
        };

        // Старт чтения задач из очереди
        taskChannel.basicConsume(TASK_QUEUE_NAME, false, consumer);
    }
}
