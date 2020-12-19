insert into t (col) values ('x', 'a', 'aargh!');

-- params
find: 'a%'
-- want
'a'
'aargh'

-- params
find: ''
-- want

