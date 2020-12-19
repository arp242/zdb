create table people (
	person_id      serial    not null,
	species_id     integer   not null  check(species_id > 0),
	faction_id     integer   null      check(faction_id is null or faction_id > 0),
	name           varchar   not null,
	title          varchar   not null,
	status         varchar   not null,

	created_at     timestamp not null  default current_timestamp
);

create table species (
	species_id     serial    not null,
	name           varchar   not null  check(name <> '')
);

create table factions (
	faction_id     serial    not null,
	name           varchar   not null  check(name <> '')
);

insert into factions
	(faction_id,  name)
values
	(1,           'Peacekeepers'),
	(2,           'Moya');

insert into species
	(species_id,  name)
values
	(1,           'Banik'),
	(2,           'Delvian'),
	(3,           'Human'),
	(4,           'Hynerian'),
	(5,           'Luxan'),
	(6,           'Pilot'),
	(7,           'Sebecian'),
	(8,           'Nebari'),
	(9,           'Pilot');

insert into people
	(name,          title,        status,     species_id,  faction_id)
values
	('Aeryn',       'Officer',    'alive',    7,           1),
	('Chiana',      '',           'alive',    8,           2),
	('Crais',       'Captain',    'alive',    7,           1),
	('Crichton',    'Astronaut',  'alive',    3,           2),
	('D''argo',     'General',    'alive',    5,           2),
	('Pilot',       'Pilot',      'alive',    9,           2),
	('Rygel',       'Dominar',    'alive',    4,           2),
	('Stark',       'Stykera',    'fahrbot',  1,           2),
	('Zhaan',       'Priest',     'dead',     2,           2);

