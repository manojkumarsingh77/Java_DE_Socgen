import java.util.List;
import java.util.Spliterator;

public class SpliteratorSplitDemo {

    public static void main(String[] args) {

        List<Integer> transactions = List.of(
                10000, 25000, 50000, 75000,
                90000, 120000, 150000, 200000
        );

        Spliterator<Integer> spliterator1 = transactions.spliterator();

        Spliterator<Integer> spliterator2 = spliterator1.trySplit();

        System.out.println("First Spliterator:");
        spliterator1.forEachRemaining(System.out::println);

        System.out.println("\nSecond Spliterator:");
        spliterator2.forEachRemaining(System.out::println);
    }
}