drop schema public; -- fast way to drop triggers
create schema public;
-- Drop pg_ads tables
DO
$do$
DECLARE
   _tbl text;
BEGIN
FOR _tbl  IN
    SELECT quote_ident(table_schema) || '.'
        || quote_ident(table_name)      -- escape identifier and schema-qualify!
    FROM   information_schema.tables
    WHERE  table_name LIKE 'pg_ads%'
LOOP
   RAISE NOTICE '%',
-- EXECUTE
  'DROP TABLE ' || _tbl;  -- see below
  EXECUTE 'DROP TABLE ' || _tbl;
END LOOP;
END
$do$;

drop table if exists parts;
create table parts (
  id bigserial,
  price real,
  name text
);

\! ./create_pg_ads_sql.rb
\i sql/pg_ads_sql.sql

select * from pg_ads__tbl_parts_max_price_10;
select * from pg_ads__tbl_parts_min_price_0_1;
select * from pg_ads__tbl_parts_avg_price_0_1__10;

insert into parts (price, name) values
('0.59', '5/16"x4" Star Drive Screw')
,('4.56', '5/16"x4" Star Drive Screw')
,('1.26', '5/16"x4" Star Drive Screw')
,('3.46', '5/16"x4" Star Drive Screw')
,('4.16', '5/16"x4" Star Drive Screw')
,('4.06', '5/16"x4" Star Drive Screw')
,('14.56', '5/16"x4" Star Drive Screw')
,('0.01', '5/16"x4" Star Drive Screw')
,('4.12', '5/16"x4" Star Drive Screw')
;

delete from parts where id = 7;
update  parts set price = 100.0 where id = 8;
select * from pg_ads__tbl_parts_min_price;
select * from pg_ads__tbl_parts_max_price;
select * from pg_ads__tbl_parts_avg_price;
