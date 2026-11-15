create database CS2_DWH

go

if OBJECT_ID('cs2_player_stats') is not null
	drop table cs2_player_stats;

go

create table cs2_player_stats(
	player_id int identity(1,1) primary key,
    player_handle nvarchar(50),
	real_name nvarchar(100),
	country_iso3 nvarchar(5),
	primary_role nvarchar(50),
	peak_hltv_rating float,
	career_kd_ratio float,
	career_headshot_pct float,
	years_active_start int,
	years_active_end int,
	years_active int,
	estimated_maps_played int,
	estimated_career_kills int,
	estimated_earnings_usd int,
	major_tournament_wins int,
	creation_date date
);

go

if OBJECT_ID('rejected_cs2_player_stats') is not null
	drop table rejected_cs2_player_stats;

go

create table rejected_cs2_player_stats(
	player_id int identity(1,1) primary key,
    player_handle nvarchar(50),
	real_name nvarchar(100),
	country_iso3 nvarchar(5),
	primary_role nvarchar(50),
	peak_hltv_rating float,
	career_kd_ratio float,
	career_headshot_pct float,
	years_active_start int,
	years_active_end int,
	years_active int,
	estimated_maps_played int,
	estimated_career_kills int,
	estimated_earnings_usd int,
	major_tournament_wins int,
	creation_date date,
	rejection_reason nvarchar(50)
)

