DROP VIEW IF EXISTS highestAvgRating ;

CREATE VIEW highestAvgRating AS 
SELECT movieid, AVG(rating) AS AverageRating, count(rating) AS ratingCount  
FROM ratings 
GROUP BY movieid
ORDER BY AverageRating DESC;

SELECT n.title, AverageRating 
FROM highestAvgRating h JOIN names n ON h.movieid = n.movieid
WHERE ratingCount > 10;