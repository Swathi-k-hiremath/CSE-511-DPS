
/* Q1: Write a query to return the total number of movies for each genre */

CREATE TABLE query1 AS 
SELECT g.name, count(1) as moviecount 
FROM hasagenre 
JOIN genres g USING (genreid) 
GROUP BY name;


/* Q2: Write a query to return the average rating per genre */

CREATE TABLE query2 AS
SELECT g.name AS name, avg(rating) AS rating
FROM movies m
JOIN hasagenre h USING (movieid)
JOIN ratings r USING (movieid)
JOIN genres g USING (genreid)
GROUP BY g.name;

/* Q3: Write a query to return the movies have at least 10 ratings */

CREATE TABLE query3 AS
SELECT m.name as title, count(1) countofratings
FROM ratings
JOIN movies m USING (movieid)
GROUP BY m.name
HAVING count(1) > 9;

/* Q4: Write a query to return all “Comedy” movies, including movieid and title. */

CREATE TABLE query4 AS
SELECT m.movieid AS movieid, m.name AS title
FROM movies m
JOIN hasagenre h USING (movieid)
JOIN genres g USING (genreid)
WHERE g.name = 'Comedy';

/* Q5: Write a query to return the average rating per movie */

CREATE TABLE query5 AS
SELECT m.name as title, avg(rating) as average
FROM ratings r
JOIN movies m USING (movieid)
GROUP BY m.name;

/* Q6: Write a query to return the average rating for all “Comedy” movies. */

CREATE TABLE query6 AS
SELECT avg(r.rating) AS average
FROM movies m
JOIN ratings r USING (movieid)
JOIN hasagenre h USING (movieid)
JOIN genres g USING (genreid)
WHERE g.name = 'Comedy';

/* Q7: Write a query to return the average rating for all movies having genre both Comedy and Romance*/

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


/* Q8: Write a query to return the average rating for all movies having genre "Romance" but not "Comedy" */

CREATE TABLE query8 AS
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


/* Q9: Write a query to return all movies that are rated by a User such that the userId is equal to v1. The v1 will be an integer parameter passed to the SQL query */

CREATE TABLE query9 AS
SELECT movieid, rating
FROM ratings
WHERE userid = :v1;
