create database if not exists stationDb;

use stationDb;

create table if not exists station_info(
Id varchar(50) primary key,
Name varchar(100),
Sector varchar(100),
Status varchar(20),
CreatedAt datetime
);

create table if not exists exception_info(
Id BIGINT AUTO_INCREMENT primary key,
EventId varchar(100) not null,
SourceId varchar(50) not null,
Value double not null,
Mean double not null,
StandardDeviation double not null,
ZScore double,
Severity varchar(20) not null,
Status varchar(20) not null check (Status in ('New', 'Investigating', 'Resolved')),
DetectedAt datetime not null,
constraint fk_source foreign key(SourceId) references station_info(Id) on delete cascade
);

create table if not exists Notification_log(
Id bigint AUTO_INCREMENT primary key,
AnomalyId bigint not null,
SentAt datetime not null default current_timestamp,
Attempts int not null,
constraint fk_anomaly foreign key (AnomalyId) references exception_info(Id) on delete cascade
);