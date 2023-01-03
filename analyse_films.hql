drop database if exists films cascade;
create database films;
use films;
CREATE EXTERNAL TABLE IF NOT EXISTS title_basics_ext (
 film_id STRING,
 title_type STRING,
 primary_title STRING,
 original_title STRING,
 is_adult BOOLEAN,
 start_year INT,
 end_year INT,
 runtime_minutes INT,
 genres STRING)
 COMMENT 'title basics'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE
 location '${input_dir4}';

CREATE TABLE IF NOT EXISTS title_basics_orc (
 film_id STRING,
 title_type STRING,
 primary_title STRING,
 original_title STRING,
 is_adult BOOLEAN,
 start_year INT,
 end_year INT,
 runtime_minutes INT,
 genres STRING)
 COMMENT 'title basics'
 STORED AS ORC;

INSERT OVERWRITE TABLE title_basics_orc 
 SELECT * FROM title_basics_ext WHERE film_id != 'tconst' AND title_type='movie';

CREATE EXTERNAL TABLE IF NOT EXISTS counted_actors_ext (
 film_id STRING,
 number_of_actors INT)
 COMMENT 'counted actors'
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\t'
 STORED AS TEXTFILE
 location '${input_dir3}';

CREATE TABLE IF NOT EXISTS counted_actors_orc (
 film_id STRING,
 number_of_actors INT)
 COMMENT 'counted_actors'
 STORED AS ORC;

INSERT OVERWRITE TABLE counted_actors_orc 
 SELECT * FROM counted_actors_ext;

CREATE OR REPLACE VIEW splitted_title_basics AS
 SELECT * FROM title_basics_orc LATERAL VIEW OUTER EXPLODE(SPLIT(genres, ',')) genres1 AS genre;

CREATE EXTERNAL TABLE IF NOT EXISTS result_table (
 genre STRING,
 films_number INT,
 actors_number INT)
 ROW FORMAT SERDE
 'org.apache.hadoop.hive.serde2.JsonSerDe'
 STORED AS TEXTFILE
 location '${output_dir6}';

INSERT OVERWRITE TABLE result_table
 SELECT genre, COUNT(*) AS films_number, SUM(COALESCE(number_of_actors, 0)) AS actors_number FROM splitted_title_basics s
 LEFT JOIN counted_actors_orc c ON s.film_id=c.film_id GROUP BY genre ORDER BY actors_number DESC LIMIT 3;
 






