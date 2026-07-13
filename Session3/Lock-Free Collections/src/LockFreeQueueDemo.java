import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LockFreeQueueDemo {

    private static final Queue<Integer> queue =
            new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws Exception {

        Runnable producer = () -> {

            for (int i = 0; i < 1000; i++) {
                queue.add(i);
            }
        };

        long start = System.currentTimeMillis();

        Thread t1 = new Thread(producer);
        Thread t2 = new Thread(producer);
        Thread t3 = new Thread(producer);

        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();

        long end = System.currentTimeMillis();

        System.out.println("Queue size: " + queue.size());
        System.out.println("Time: " + (end - start) + " ms");
    }
}