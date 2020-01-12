ratings = LOAD '/user/maria_dev/ml-100/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100/u.item' USING PigStorage('|') 
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS numberOfRatings;

oneStarMovies = FILTER avgRatings BY avgRating < 2.0;

oneStarsWithData = JOIN oneStarMovies BY movieID, nameLookup BY movieID;

mostRatedOneStarMovie = FOREACH oneStarsWithData GENERATE nameLookup::movieTitle AS movieName,
	oneStarMovies::avgRating AS AvgRating, oneStarMovies::numberOfRatings AS NumOfRatings;

mostRatedOneStarMovieSorted = ORDER mostRatedOneStarMovie BY NumOfRatings DESC;

DUMP mostRatedOneStarMovieSorted;
