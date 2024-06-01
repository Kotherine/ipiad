package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/*
 * Главный класс приложения.
 */
public class Main {

    /*
     * Экземпляр логгера для записи событий приложения.
     */
    private static final Logger logger = LogManager.getLogger(Main.class);

    /*
     * Точка входа в приложение.
     */
    public static void main(String[] args) {
        ElasticSearchUtil dbConnector = null;

        try {
            // Создание экземпляра коннектора для Elasticsearch и его инициализация логгером.
            dbConnector = new ElasticSearchUtil(logger);

//            // Вызов метода поиска
//            List<News> results = dbConnector.searchDocuments("лето", "ДК Коммунарка", "31 мая 2024");
//
//            // Обработка результатов
//            for (News news : results) {
//                System.out.println(news);
//            };
//
//            // Вызов метода агрегации
//            dbConnector.termsAggregation();

            // Запуск потока-производителя.
            Thread producerThread = new Thread(() -> {
                try {
                    CrawlerTaskProducer.main(args);
                } catch (Exception e) {
                    logger.error("Error in producer thread: " + e.getMessage(), e);
                }
            });

            // Запуск потока-обработчика.
            Thread workerThread = new Thread(() -> {
                try {
                    CrawlerWorker.main(args);
                } catch (Exception e) {
                    logger.error("Error in worker thread: " + e.getMessage(), e);
                }
            });

            // Запуск потока-потребителя.
            Thread consumerThread = new Thread(() -> {
                try {
                    CrawlerConsumer.main(args);
                } catch (Exception e) {
                    logger.error("Error in consumer thread: " + e.getMessage(), e);
                }
            });

            // Запуск всех потоков.
            producerThread.start();
            workerThread.start();
            consumerThread.start();

            // Ожидание завершения всех потоков.
            producerThread.join();
            workerThread.join();
            consumerThread.join();

        } catch (Exception e) {
            // Обработка любых исключений, возникающих в основном потоке.
            logger.error("Error in main: " + e.getMessage(), e);
        } finally {
            // Закрытие экземпляра коннектора для Elasticsearch.
            if (dbConnector != null) {
                dbConnector.close();
            }
        }
    }
}
