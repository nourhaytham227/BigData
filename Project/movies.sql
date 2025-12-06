select * from movies;
select * from movie_stats;
delete from movies;
DELETE FROM MOVIE_STATS;
commit;
SELECT COUNT(*) FROM MOVIES;
select sum(movie_count) from movie_stats;
select sum(movie_count) from movie_stats where genre='Comedy'

commit