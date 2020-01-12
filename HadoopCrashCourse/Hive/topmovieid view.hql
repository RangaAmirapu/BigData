CREATE VIEW topMovieIDs AS 
SELECT movieid, count(movieid) AS ratingCount
FROM ratings 
GROUP BY movieid
ORDER BY ratingCount DESC;

SELECT n.title, ratingCount 
FROM topMovieIDs t JOIN names n ON t.movieid = n.movieid;
