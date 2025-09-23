-- create the streaming table
CREATE OR REFRESH STREAMING TABLE silver_movies;

CREATE TEMPORARY VIEW silver_imdb_movies AS
  SELECT 
    --CAST(REGEXP_REPLACE(tconst, '^tt0*', '') AS STRING) as id, 
    tconst as id,
    wallTime,
    primaryTitle as title, 
    split(genres, ',') as genres, 
    startYear as year, 
    "IMDb Non-Commercial Datasets" as source,
    "insert" as operationType 
  FROM STREAM(bronze_imdb_movies);


CREATE TEMPORARY VIEW silver_mongodb_movies AS
  SELECT 
    _id as id, 
    wallTime,
    title, 
    genres, 
    year, 
    "mongodb_sample_mflix" as source,
    operationType
  FROM STREAM(bronze_mongodb_movies);

CREATE FLOW mongodb_cdc_movies AS
AUTO CDC INTO silver_movies
FROM STREAM(silver_mongodb_movies)
KEYS (id)
APPLY AS DELETE WHEN operationType = "delete"
SEQUENCE BY wallTime;

CREATE FLOW imdb_cdc_movies AS
AUTO CDC INTO silver_movies
FROM STREAM(silver_imdb_movies)
KEYS (id)
APPLY AS DELETE WHEN operationType = "delete"
SEQUENCE BY wallTime;

CREATE OR REFRESH MATERIALIZED VIEW report_primarykey_tests(
  CONSTRAINT unique_pk EXPECT (num_entries = 1)
)
AS 
SELECT id, count(*) as num_entries
FROM silver_movies
GROUP BY id
