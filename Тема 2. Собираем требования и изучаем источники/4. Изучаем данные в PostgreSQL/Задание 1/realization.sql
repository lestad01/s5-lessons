select 
	  t.table_name,
	  t.column_name,
	  t.data_type,
	  t.character_maximum_length,
	  t.column_default,
	  t.is_nullable

from information_schema.columns t
where t.table_schema = 'public' 
order by 1;