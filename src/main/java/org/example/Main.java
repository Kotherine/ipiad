package org.example;

public class Main {
    public static void main(String[] args) throws Exception {
        // Запускаем producer для заполнения очереди задач
        CrawlerTaskProducer.main(args);

        // Запускаем worker для обработки задач в отдельном потоке
        Thread workerThread = new Thread(() -> {
            try {
                CrawlerWorker.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        workerThread.start();

        // Запускаем consumer для обработки результатов в отдельном потоке
        Thread resultThread = new Thread(() -> {
            try {
                CrawlerConsumer.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        resultThread.start();
    }
}
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
//public class Main {
//    public static void main(String[] args) throws Exception {
//        // Запускаем отправителя сообщений
//        MessageProduser.main(args);
//
//        // Запускаем получателя сообщений в отдельном потоке
//        Thread receiverThread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    MessageReceiver.main(args);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//
//        receiverThread.start();
//    }
//}