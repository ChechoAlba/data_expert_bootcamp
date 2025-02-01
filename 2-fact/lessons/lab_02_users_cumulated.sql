create table users_cumulated (
	user_id TEXT,
	dates_active DATE[], --The list of dates in the past where the user was active
	date DATE, --current date for the user
	primary key (user_id, date)
)
--drop table users_cumulated