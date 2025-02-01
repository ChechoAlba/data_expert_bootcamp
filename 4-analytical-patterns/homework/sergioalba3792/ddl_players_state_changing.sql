CREATE TABLE players_state_changing (
    player_name TEXT,
    first_active_season integer,
    last_active_season integer,
    season_active_state TEXT,
    season_active integer[],
    season integer,
	 PRIMARY KEY (player_name, season)
);