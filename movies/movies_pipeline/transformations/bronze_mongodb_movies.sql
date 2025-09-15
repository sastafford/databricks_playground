CREATE OR REFRESH STREAMING TABLE bronze_mongodb_movies;

CREATE FLOW mongodb_movies_incremental AS
INSERT INTO 
  bronze_mongodb_movies BY NAME
SELECT 
  operationType, 
  ns.*, 
  CAST(wallTime AS TIMESTAMP) AS wallTime, 
  fullDocument.* EXCEPT (awards, imdb, num_mflix_comments, released, runtime, year)
FROM STREAM(read_files(
  "/Volumes/usa/movies/temp/mongodb/incremental/",
  format => "json"
));

CREATE FLOW mongodb_movies_backfill AS 
INSERT INTO ONCE
  bronze_mongodb_movies BY NAME
SELECT *
FROM bronze_mongodb_movies_backfill;

