CREATE TABLE query1 AS 
SELECT g.name, count(1) as moviecount 
FROM hasagenre 
JOIN genres g 
USING (genreid) GROUP BY name


CREATE TABLE query2 AS
SELECT g.name AS name, avg(rating) AS rating
FROM movies m
JOIN hasagenre h USING (movieid)
JOIN ratings r USING (movieid)
JOIN genres g USING (genreid)
GROUP BY g.name;

CREATE TABLE query3 AS
SELECT m.name as title, count(1) countofratings
FROM ratings
JOIN movies m USING (movieid)
GROUP BY m.name
HAVING count(1) > 9;

CREATE TABLE query4 AS
SELECT m.movieid AS movieid, m.name AS title
FROM movies m
JOIN hasagenre h USING (movieid)
JOIN genres g USING (genreid)
WHERE g.name = 'Comedy';

CREATE TABLE query5 AS
SELECT m.name as title, avg(rating) as average
FROM ratings r
JOIN movies m USING (movieid)
GROUP BY m.name;

CREATE TABLE query6 AS
SELECT avg(r.rating) AS average
FROM movies m
JOIN ratings r USING (movieid)
JOIN hasagenre h USING (movieid)
JOIN genres g USING (genreid)
WHERE g.name = 'Comedy';

CREATE TABLE query7 AS
SELECT avg(rating) as average
FROM ratings
JOIN (SELECT movieid
	FROM movies m
	JOIN hasagenre h USING (movieid)
	JOIN genres g USING (genreid)
	WHERE g.name IN ('Romance', 'Comedy')
	GROUP BY m.movieid
	HAVING count(1) = 2) Y
USING (movieid);

SELECT AVG(rating) AS average
FROM ratings
JOIN (SELECT movieid
      FROM movies m
      JOIN hasagenre h USING (movieid)
      JOIN genres g USING (genreid)
      WHERE g.name = 'Romance' 
      AND m.movieid NOT IN (
        SELECT m.movieid
        FROM movies m
        JOIN hasagenre h USING (movieid)
        JOIN genres g USING (genreid)
        WHERE g.name = 'Comedy'
      )
      GROUP BY m.movieid
     ) Y
USING (movieid);


CREATE TABLE query9 AS
SELECT movieid, rating
FROM ratings
WHERE userid = v1;
