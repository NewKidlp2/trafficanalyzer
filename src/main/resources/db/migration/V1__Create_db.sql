create table LIMITS_PER_HOUR
(
    ID        serial primary key,
    MIN_VAL   varchar(64) not null,
    MAX_VAL   varchar(10) not null,
	DATE	  date not null
);