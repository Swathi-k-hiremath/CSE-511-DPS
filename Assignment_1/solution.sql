
/* CREATE ENTITY TABLES */

/* Create the users table with userid as primary key */

CREATE TABLE users( UserID INT PRIMARY KEY, Name TEXT NOT NULL);


/* Create the movies table with movieid as primary key */

CREATE TABLE movies(MovieID INT PRIMARY KEY, Name TEXT NOT NULL);


/* Create the taginfo table with tagid as primary key */

CREATE TABLE taginfo(TagID INT PRIMARY KEY, Content TEXT UNIQUE);


/* Create the genres table with genreid as primary key */

CREATE TABLE genres(GenreID INT PRIMARY KEY, Name TEXT UNIQUE);



/* CREATE RELATIONS TABLES */

/* ratings table references userid, movieid from respective tables and uses its combination as primary key. Time stamps are defined as bigint and rating is a number between 0 and 5 */

CREATE TABLE ratings( UserID INT REFERENCES users(UserID),
		      MovieID INT REFERENCES movies(MovieID),
		      Rating NUMERIC CHECK (0 <= Rating and Rating <= 5),
		      Timestamp BIGINT,
	   	      PRIMARY KEY(UserID, MovieID)
);


/* tags table references userid, movieid, taginfo from respective tables and uses its combination as primary key. Time stamps are defined as bigint */


CREATE TABLE tags( UserID INT REFERENCES users(UserID),
		   MovieID INT REFERENCES movies(MovieID),
		   TagID INT REFERENCES taginfo(TagID),
		   Timestamp BIGINT,
		   PRIMARY KEY(UserID, MovieID, TagID)
);

/* hasagenre table references movieid, generic from respective tables and uses its combination as primary key. */


CREATE TABLE hasagenre( MovieID INT REFERENCES movies(MovieID),
			GenreID INT REFERENCES genres(GenreID),
			PRIMARY KEY(MovieID, GenreID)
);