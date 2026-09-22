create database if not exists stationDb;
use stationDb;
create table station_info(
Id varchar(50) primary key,
Name varchar(100),
Sector varchar(100),
Status varchar(20),
CreatedAt datetime
);