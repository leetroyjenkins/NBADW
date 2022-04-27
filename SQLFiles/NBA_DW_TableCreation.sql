/*
USE MASTER

IF DB_ID (N'NBADW') IS NOT NULL
	DROP DATABASE NBADW

CREATE DATABASE NBADW

GO
*/
USE NBADW;

BEGIN TRAN create_dw_tables

	IF OBJECT_ID( 'scoring_fact_game_FK', 'F') IS NOT NULL
		ALTER TABLE ScoringFact
		DROP CONSTRAINT scoring_fact_game_FK;

	IF OBJECT_ID( 'scoring_fact_player_FK', 'F') IS NOT NULL
		ALTER TABLE ScoringFact
		DROP CONSTRAINT scoring_fact_player_FK;

	IF OBJECT_ID( 'scoring_fact_coach_FK', 'F') IS NOT NULL
		ALTER TABLE ScoringFact
		DROP CONSTRAINT scoring_fact_coach_FK;

	IF OBJECT_ID( 'scoring_fact_team_FK', 'F') IS NOT NULL
		ALTER TABLE ScoringFact
		DROP CONSTRAINT scoring_fact_team_FK;

	IF OBJECT_ID( 'scoring_fact_season_date_FK', 'F') IS NOT NULL
		ALTER TABLE ScoringFact
		DROP CONSTRAINT scoring_fact_season_date_FK;

	IF OBJECT_ID( 'ScoringFact', 'U') IS NOT NULL
		DROP TABLE ScoringFact;

	IF OBJECT_ID( 'GameDim', 'U') IS NOT NULL
		DROP TABLE GameDim;

	IF OBJECT_ID( 'PlayerDim', 'U') IS NOT NULL
		DROP TABLE PlayerDim;

	IF OBJECT_ID( 'HeadCoachDim', 'U') IS NOT NULL
		DROP TABLE HeadCoachDim;

	IF OBJECT_ID( 'TeamDim', 'U') IS NOT NULL
		DROP TABLE TeamDim;

	IF OBJECT_ID( 'SeasonDateDim', 'U') IS NOT NULL
		DROP TABLE SeasonDateDim;

	IF OBJECT_ID( 'TeamNameLookup', 'u') IS NOT NULL
		DROP TABLE TeamNameLookup;

	CREATE TABLE TeamNameLookup(teamid INT IDENTITY(1,1)
						, TeamInitials NVARCHAR(255)
						, TeamName NVARCHAR(255)
						);

	CREATE TABLE SeasonDateDim(
		season_date_dim_syn_key INT IDENTITY (1,1)
		, date_key INT
		, full_date DATE
		, [year] INT
		, [month] INT
		, [day] INT
		, day_name NVARCHAR (255)
		, day_abbreviation NVARCHAR(255)
        , day_number_in_week INT
	    , month_name NVARCHAR(255)
		, month_abbreviation NVARCHAR(255)
	    , season_start_year INT
        , month_in_season INT
        , week_number_in_season INT
	    , day_number_in_season INT
	    , season_start_date DATE
		, week_number_in_year INT
	    , season NVARCHAR(255)
	    , date_type_in_season NVARCHAR(255)
	    , weekday_flag NVARCHAR(255)
		CONSTRAINT season_date_dim_PK PRIMARY KEY (season_date_dim_syn_key)
	);

	CREATE TABLE TeamDim ( 
		team_dim_syn_key INT IDENTITY (1,1)
		, team_id NVARCHAR(255)
		, team_name NVARCHAR(255)
		, team_conference NVARCHAR(255)
		, team_division NVARCHAR(255)
		, season_start_year INT
		, season NVARCHAR(255)
		, effective_date DATE DEFAULT GETDATE()
		, inactive_record BIT DEFAULT 0
		CONSTRAINT team_dim_PK PRIMARY KEY (team_dim_syn_key)
	);

	CREATE TABLE HeadCoachDim ( 
		head_coach_dim_syn_key INT IDENTITY (1,1)
		, coach_name NVARCHAR(255)
		--, coach_first_name NVARCHAR(255)
		, coach_last_name NVARCHAR(255)
		, team_initials NVARCHAR(255)
		, team NVARCHAR(255)
		, season_start_year INT
		, age INT
		, coach_id_not_pk INT	
		, effective_date DATE DEFAULT GETDATE()
		, inactive_record BIT DEFAULT 0
		CONSTRAINT head_coach_PK PRIMARY KEY (head_coach_dim_syn_key)
	);

	CREATE TABLE PlayerDim ( 
		player_dim_syn_key INT IDENTITY (1,1)
		, player_name NVARCHAR(255)
		, player_last_name NVARCHAR(255)
		, player_first_name NVARCHAR(255)
		, date_of_birth DATE
		, height NVARCHAR(255)
		, [weight] INT
		, position NVARCHAR(255)
		, season_start_year INT
		, season_end_year INT
		, effective_date DATE DEFAULT CAST(GETDATE() AS DATE)
		, inactive_record BIT DEFAULT 0
		CONSTRAINT player_PK PRIMARY KEY (player_dim_syn_key)
	);


	CREATE TABLE GameDim ( 
		game_dim_syn_key INT IDENTITY (1,1)
		, game_id INT
		, home_team NVARCHAR(255)
		, away_team NVARCHAR(255)
		, home_points INT
		, away_points INT
		, season_start_year INT
		, game_date DATE
		, start_time NVARCHAR(255)
		, attendance INT
		, regular_season_game BIT
		CONSTRAINT game_PK PRIMARY KEY (game_dim_syn_key)
	);

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

	CREATE TABLE ScoringFact ( 
		scoring_fact_syn_key INT IDENTITY (1,1)
		, game_dim_syn_key INT
		, player_dim_syn_key INT
		, head_coach_dim_syn_key INT
		, team_dim_syn_key INT
		, season_date_dim_syn_key INT
		, points_scored INT
		, win BIT
		, field_goals INT
		, field_goal_attempts INT
		, three_pointers INT
		, three_point_attempts INT
		, free_throws INT
		, free_throw_attempts INT
		, CONSTRAINT scoring_fact_PK PRIMARY KEY (scoring_fact_syn_key)
	);

COMMIT TRAN create_dw_tables;
--ROLLBACK create_dw_tables;

GO

USE master