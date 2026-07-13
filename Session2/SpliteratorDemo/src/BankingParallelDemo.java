import java.util.List;

public class BankingParallelDemo {

    public static void main(String[] args) {

        List<Double> transactions = List.of(
                10000.0, 50000.0, 90000.0, 120000.0,
                7000.0, 45000.0, 88000.0, 150000.0
        );

        transactions.parallelStream()
                .filter(amount -> {
                    System.out.println(
                            Thread.currentThread().getName()
                                    + " processing " + amount
                    );
                    return amount > 50000;
                })
                .forEach(System.out::println);
    }
}