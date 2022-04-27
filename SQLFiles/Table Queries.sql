USE NBADW


SELECT  *
FROM TeamNameLookup;

SELECT *
FROM SeasonDateDim;

SELECT *
FROM PlayerDim

SELECT *
FROM HeadCoachDim
ORDER BY coach_last_name DESC;

SELECT  *
FROM GameDim
ORDER BY game_id;

SELECT *
FROM TeamDim;

SELECT TOP (1000) *
FROM ScoringFact;


--SELECT TOP(100) * 
--FROM PlayStaging;

--SELECT COUNT(play_id)
--FROM PlayStaging;

--SELECT *
--FROM TeamDim
--WHERE team_name = 'Washington Bullets'

--SELECT DISTINCT(season_start_year)
--FROM TeamDim
--ORDER BY season_start_year;


--SELECT *
--FROM PlayerDim
--WHERE player_last_name = 'Radojević';

--SELECT *
--FROM HeadCoachDim
--WHERE team = 'Sacramento Kings'
--ORDER BY season_start_year;

GO

USE master
go