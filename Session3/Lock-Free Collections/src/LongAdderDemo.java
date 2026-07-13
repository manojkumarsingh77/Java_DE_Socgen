import java.util.concurrent.atomic.LongAdder;

public class LongAdderDemo {

    private static final LongAdder counter = new LongAdder();

    public static void main(String[] args) throws Exception {

        Runnable task = () -> {
            for (int i = 0; i < 100000; i++) {
                counter.increment();
            }
        };

        Thread t1 = new Thread(task);
        Thread t2 = new Thread(task);
        Thread t3 = new Thread(task);

        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();

        System.out.println(counter.sum());
    }
}