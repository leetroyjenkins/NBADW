USE NBADW

BEGIN TRAN add_fks
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

	ALTER TABLE ScoringFact
	ADD CONSTRAINT scoring_fact_game_FK FOREIGN KEY (game_dim_syn_key) REFERENCES GameDim(game_dim_syn_key)

	ALTER TABLE ScoringFact
	ADD CONSTRAINT scoring_fact_player_FK FOREIGN KEY (player_dim_syn_key) REFERENCES PlayerDim(player_dim_syn_key)

	--ALTER TABLE ScoringFact
	--ADD CONSTRAINT scoring_fact_coach_FK FOREIGN KEY (head_coach_dim_syn_key) REFERENCES HeadCoachDim(head_coach_dim_syn_key)

	--ALTER TABLE ScoringFact
	--ADD CONSTRAINT scoring_fact_team_FK FOREIGN KEY (team_dim_syn_key) REFERENCES TeamDim(team_dim_syn_key)

	ALTER TABLE ScoringFact
	ADD CONSTRAINT scoring_fact_season_date_FK FOREIGN KEY (season_date_dim_syn_key) REFERENCES SeasonDateDim(season_date_dim_syn_key)

COMMIT TRAN add_fks;

exec sp_help ScoringFact