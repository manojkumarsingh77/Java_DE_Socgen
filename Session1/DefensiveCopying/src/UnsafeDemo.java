import java.util.ArrayList;
import java.util.List;

public class UnsafeDemo {

    public static void main(String[] args) {

        List<String> myMovies = new ArrayList<>();
        myMovies.add("Inception");
        myMovies.add("Interstellar");
        myMovies.add("Memento");

        UnsafeMovieWatchList watchList =
                new UnsafeMovieWatchList(myMovies);

        System.out.println("Before attack: " + watchList.getMovies());

        List<String> attacker = watchList.getMovies();
        attacker.clear();

        System.out.println("After attack: " + watchList.getMovies());
    }
}