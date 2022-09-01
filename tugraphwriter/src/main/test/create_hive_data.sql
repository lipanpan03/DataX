CREATE TABLE movies (
                        mid STRING,
                        name STRING,
                        year INT,
                        rate FLOAT)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


insert into
    movies (mid, name, year, rate)
values
    ('tt0188766', 'King of Comedy', 1999, 7.3),
    ('tt0286112', 'Shaolin Soccer', 2001, 7.3),
    ('tt0286112', 'The Mermaid',   2016,  6.3);


CREATE TABLE movie (
    id int,
    duration int,
    title string,
    tagline string,
    summary string,
    poster_image string,
    rated string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO movie
( id, duration, title, tagline, summary, poster_image, rated )
VALUES
( 100001, 11, "title1", "tagline1", "summary1", "poster_image1", "rated1" ),
( 100002, 12, "title2", "tagline2", "summary2", "poster_image2", "rated2" ),
( 100003, 13, "title3", "tagline3", "summary3", "poster_image3", "rated3" );
