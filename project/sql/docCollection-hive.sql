CREATE DATABASE DocCollection;

USE DocCollection;

--- source 1 ----
CREATE EXTERNAL TABLE DocMetadataSource1 (pubdate STRING, year INT, month INT, author STRING,
                                    title STRING, section STRING, publication STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION 's3://ctezna-docstore/preprocessed/metadata/source1/';

SELECT * FROM DocMetadataSource1 LIMIT 10;

SELECT DISTINCT year
FROM DocMetadataSource1
WHERE year IS NOT NULL;

SELECT year, COUNT(*)
FROM DocMetadataSource1
WHERE year IS NOT NULL
GROUP BY year;

SELECT count(DISTINCT publication)
FROM DocMetadataSource1;

--- source 2 ------

CREATE EXTERNAL TABLE DocMetadataSource2 (title STRING, publication STRING, author STRING,
                                    pubdate STRING, year INT, month INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION 's3://ctezna-docstore/preprocessed/metadata/source2/';

SELECT * FROM docmetadatasource2 LIMIT 10;

SELECT year, COUNT(*) FROM docmetadatasource2
GROUP BY year;

SELECT * FROM docmetadatasource2
WHERE year IS NULL
LIMIT 10;

SELECT DISTINCT month FROM docmetadatasource2;

SELECT title, month 
FROM docmetadatasource2
WHERE year IS NULL AND month > 2000;

--- source 3 ----

CREATE EXTERNAL TABLE DocMetadataSource3 (author STRING, claps STRING, reading_time INT,
                                    title STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION 's3://ctezna-docstore/preprocessed/metadata/source3/';

SELECT * FROM docmetadatasource3 LIMIT 10;

SELECT title, reading_time FROM docmetadatasource3
ORDER BY reading_time DESC LIMIT 10;

--- source 5 ---

CREATE EXTERNAL TABLE DocMetadataSource5 (author STRING, title STRING, highlighttext STRING,
                                    language STRING, published DATE, crawled DATE) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION 's3://ctezna-docstore/preprocessed/metadata/source5/';

SELECT * FROM docmetadatasource5 LIMIT 10;

--- all docs ----

CREATE EXTERNAL TABLE Docs (text STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION 's3://ctezna-docstore/preprocessed/docs/';


SELECT * FROM Docs;

SELECT COUNT(*) FROM Docs;

SELECT word, count(1) AS count FROM 
(SELECT explode(split(text,' ')) AS word FROM Docs) w 
GROUP BY word 
ORDER BY word DESC LIMIT 10;

SELECT word, count(1) AS count FROM 
(SELECT explode(split(text,' ')) AS word FROM Docs) w 
GROUP BY word 
ORDER BY count DESC LIMIT 10;