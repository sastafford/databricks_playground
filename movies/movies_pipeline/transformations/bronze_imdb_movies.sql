CREATE OR REFRESH STREAMING TABLE bronze_imdb_movies;

-- append the original incremental, streaming flow
CREATE FLOW
  imdb_movies_incremental
AS INSERT INTO
  bronze_imdb_movies BY NAME
SELECT * 
FROM STREAM read_files(
  "/Volumes/usa/movies/temp/imdb/incremental/",
  format => "csv",
  sep => "\t",
  header => true,
  inferColumnTypes => true,
  maxFilesPerTrigger => 100,
  schema => "tconst STRING, titleType STRING, primaryTitle STRING,originalTitle STRING, isAdult INT, startYear STRING, endYear STRING,runtimeMinutes STRING, genres STRING"
);

CREATE FLOW imdb_movies_backfill
AS INSERT INTO ONCE
  bronze_imdb_movies BY NAME
SELECT *
FROM read_files(
  "/Volumes/usa/movies/temp/imdb/backfill/",
  format => "csv",
  sep => "\t",
  header => true
);