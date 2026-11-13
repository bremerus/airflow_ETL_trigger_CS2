create database WeatherDWH;

go

use WeatherDWH;
go

create table silver_weather_olympus (
	weather_date date not null,
	location varchar(50) not null,
	elevation_m float,
	max_temp_c float,
	min_temp_c float,
	precipitation_mm float,
	max_wind_kmh float,
	loaded_at datetime default getdate(),
	primary key (weather_date, location)
);
go

create table rejected_weather_olympus(
	rejection_id int identity(1,1) primary key,
	weather_date date,
	location varchar(50),
	elevation_m float,
	max_temp_c float,
	min_temp_c float,
	precipitation_mm float,
	max_wind_kmh float,
	loaded_at datetime default getdate(),
	rejection_reason VARCHAR(255)
)
