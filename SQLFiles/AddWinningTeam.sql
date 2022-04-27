/*USE NBADW


GO

ALTER TABLE GameDim
ADD winning_team AS CAST
(
CASE
WHEN home_points > away_points THEN home_team
ELSE away_team
END AS NVARCHAR(255)
) PERSISTED

GO
*/
