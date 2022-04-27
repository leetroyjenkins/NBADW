/*SELECT DB_NAME(dbid) AS NBADW,
COUNT(dbid) AS NumberOfConnections,
loginame
INTO Original_Connections
FROM    sys.sysprocesses
GROUP BY dbid, loginame
ORDER BY DB_NAME(dbid);

GO
*/


SELECT * FROM Original_Connections;

SELECT DB_NAME(dbid) AS NBADW
, COUNT(dbid) AS NumberOfConnections
, loginame
FROM    sys.sysprocesses AS s
GROUP BY dbid, loginame
ORDER BY DB_NAME(dbid);



SELECT @@MAX_CONNECTIONS;