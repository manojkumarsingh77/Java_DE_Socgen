import java.util.ArrayList;
import java.util.List;

public class SafeMovieWatchList {

    private final List<String> movies;

    public SafeMovieWatchList(List<String> movies) {
        this.movies = new ArrayList<>(movies);
    }

    public List<String> getMovies() {
        return new ArrayList<>(movies);
    }
}