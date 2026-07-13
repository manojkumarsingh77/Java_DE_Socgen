import java.util.List;

public class UnsafeMovieWatchList {

    private final List<String> movies;

    public UnsafeMovieWatchList(List<String> movies) {
        this.movies = movies;
    }

    public List<String> getMovies() {
        return movies;
    }
}