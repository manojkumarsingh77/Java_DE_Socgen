import java.util.List;
import java.util.Spliterator;

public class SpliteratorBasicDemo {

    public static void main(String[] args) {

        List<String> customers = List.of(
                "Rahul",
                "Amit",
                "Neha",
                "John"
        );

        Spliterator<String> spliterator = customers.spliterator();

        System.out.println("Estimate Size: " + spliterator.estimateSize());

        spliterator.tryAdvance(System.out::println);
        spliterator.tryAdvance(System.out::println);

        System.out.println("Remaining:");

        spliterator.forEachRemaining(System.out::println);
    }
}