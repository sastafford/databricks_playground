CREATE MATERIALIZED VIEW gold_genre_analysis AS
    SELECT genre, COUNT(genre) as sum
    FROM (
        SELECT explode(genres) as genre
        FROM usa.movies.silver_movies
    )
    GROUP BY genre
    ORDER BY sum DESC
    LIMIT 10