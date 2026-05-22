import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BackpressureDemo {

    public static void main(String[] args) {

        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(10);

        Thread producer = new Thread(() -> {

            int count = 0;

            while (true) {
                try {
                    queue.put(count++);
                    System.out.println("Produced. Queue size: " + queue.size());

                    Thread.sleep(50);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        Thread consumer = new Thread(() -> {

            while (true) {
                try {
                    Integer item = queue.take();

                    Thread.sleep(300);

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