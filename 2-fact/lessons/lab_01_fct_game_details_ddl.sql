create table fct_game_details (
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
	dim_player_name TEXT,
	dim_start_position text,
	dim_is_playing_at_home BOOLEAN,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_minutes real,
	m_gfm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm integer,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnover INTEGER,
	m_pf INTEGER,
	m_points INTEGER,
	m_plus_minus INTEGER,
	primary KEY(dim_game_date, dim_team_id, dim_player_id)
)