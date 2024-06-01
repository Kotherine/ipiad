package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
 * The main class of the application.
 */
public class Main {

    /*
     * Logger instance for logging application events.
     */
    private static final Logger logger = LogManager.getLogger(Main.class);

    /*
     * The entry point of the application.
     */
    public static void main(String[] args) {
        ElasticSearchUtil dbConnector = null;

        try {
            // Creating a connector instance for Elasticsearch and initializing it with the logger.
            dbConnector = new ElasticSearchUtil(logger);

            // Starting the producer thread.
            Thread producerThread = new Thread(() -> {
                try {
                    CrawlerTaskProducer.main(args);
                } catch (Exception e) {
                    logger.error("Error in producer thread: " + e.getMessage(), e);
                }
            });

            // Starting the worker thread.
            Thread workerThread = new Thread(() -> {
                try {
                    CrawlerWorker.main(args);
                } catch (Exception e) {
                    logger.error("Error in worker thread: " + e.getMessage(), e);
                }
            });

            // Starting the consumer thread.
            Thread consumerThread = new Thread(() -> {
                try {
                    CrawlerConsumer.main(args);
                } catch (Exception e) {
                    logger.error("Error in consumer thread: " + e.getMessage(), e);
                }
            });

            // Starting all threads
            producerThread.start();
            workerThread.start();
            consumerThread.start();

            // Waiting for all threads to finish
            producerThread.join();
            workerThread.join();
            consumerThread.join();

        } catch (Exception e) {
            // Handling any exceptions that occur in the main thread.
            logger.error("Error in main: " + e.getMessage(), e);
        } finally {
            // Closing the Elasticsearch connector instance.
            if (dbConnector != null) {
                dbConnector.close();
            }
        }
    }
}
