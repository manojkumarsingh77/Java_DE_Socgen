import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class NoBackpressureDemo {

    public static void main(String[] args) {

        BlockingQueue<Integer> queue = new LinkedBlockingQueue<>();

        Thread producer = new Thread(() -> {

            int count = 0;

            while (true) {
                try {
                    queue.put(count++);
                    System.out.println("Produced. Queue size: " + queue.size());

                    Thread.sleep(50); // fast producer

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        Thread consumer = new Thread(() -> {

            while (true) {
                try {
                    Integer item = queue.take();

                    Thread.sleep(300); // slow consumer

                    System.out.println("Consumed: " + item);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        producer.start();
        consumer.start();
    }
}