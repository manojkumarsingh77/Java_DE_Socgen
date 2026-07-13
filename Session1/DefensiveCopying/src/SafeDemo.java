import java.util.ArrayList;
import java.util.List;

public class SafeDemo {

    public static void main(String[] args) {

        List<String> myMovies = new ArrayList<>();
        myMovies.add("Inception");
        myMovies.add("Interstellar");
        myMovies.add("Memento");

        SafeMovieWatchList watchList =
                new SafeMovieWatchList(myMovies);

        System.out.println("Before modification: " + watchList.getMovies());

        List<String> attacker = watchList.getMovies();
        attacker.clear();

        System.out.println("Attacker modified returned copy: " + attacker);

        System.out.println("Original internal data still safe: "
                + watchList.getMovies());
    }
}