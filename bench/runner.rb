#!/usr/bin/env ruby
PORT = 5433 # Change to 5432 before pushing to git!
SQL_PREAMBLE = <<~SQL
-- Drop pg_ads tables
DO
$do$
DECLARE
   _tbl text;
BEGIN
FOR _tbl  IN
    SELECT quote_ident(table_schema) || '.'
        || quote_ident(table_name)
    FROM   information_schema.tables
    WHERE  table_name LIKE 'pg_ads%'
LOOP
   RAISE NOTICE '%',
  'DROP TABLE ' || _tbl;
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
SQL

def insert
  "insert into parts (price, name) values (#{rand(0.01..1000.00)}, #{(0...8).map { (65 + rand(26)).chr }.join});"
end

def delete
  <<~SQL
  DELETE FROM parts WHERE id = (
    SELECT id FROM parts ORDER BY random() LIMIT 1
  );
  SQL
end

def update
  <<~SQL
  UPDATE parts SET price = #{rand(0.01..1000.00)} WHERE id = (
    SELECT id FROM parts ORDER BY random() LIMIT 1
  );
  SQL
end

def alert(alert_type)
  if alert_type == 'random'
    alert_type = ['min', 'max', 'avg'].sample
  end
  <<~YAML
- name: New highest part price
  table: parts
  aggregate: #{alert_type}
  column: price
  column_type: real
  threshold: #{rand(0.01..1000.00)}
  YAML
end

def run_experiment(exp_type, operation_count, alert_count, alert_type = 'max')
  puts "GENERATING..."
  ops = case exp_type
  when 'INSERT'
    (1..operation_count).map { insert }
  when 'UPDATE'
    (1..operation_count).map { update }
  when 'DELETE'
    (1..operation_count).map { delete }
  when 'MIXED'
    (1..operation_count).map { [insert, update, delete].sample }
  end
  alerts = (1..alert_count).map { alert(alert_type) }

  puts "GENERATED, WRITING..."
  File.open('/tmp/preamble.sql', 'w') { |f| f.write(SQL_PREAMBLE) }
  File.open('/tmp/alerts.yml', 'w') { |f| f.write(alerts.join("\n")) }
  `./ruby_version.rb /tmp/alerts.yml > /tmp/alert_sql.sql 2>/dev/null`
  File.open('/tmp/tmp.sql', 'w') { |f| f.write(ops.join("\n")) }
  puts "WRITTEN, PREPPING DB"
  `psql -p #{PORT} -f /tmp/preamble.sql > /dev/null 2>&1`
  `psql -p #{PORT} -f /tmp/alert_sql.sql > /dev/null 2>&1`
  # `time psql -p #{PORT} -f /tmp/tmp.sql > /dev/null 2>&1`
  puts "DB PREPPED, GO TIME"
  puts("type: #{exp_type}, op_count #{operation_count}, alert_count #{alert_count}, alert_type #{alert_type}")
  `/usr/bin/time -f "%e" sh -c '"$0" "$@" >/dev/null 2>&1' psql -p #{PORT} -f /tmp/tmp.sql`
  puts "NEXT"
end

exp_types = ['UPDATE', 'INSERT', 'DELETE', 'MIXED']
# op_counts = [1000, 10_000, 2_000_000]
# alert_counts = [0, 1, 10, 1000, 10_000, 100_000]
op_counts = [10_000]
alert_counts = [10_000]
alert_types = ['max', 'min', 'avg', 'random']
exp_types.each do |exp_type|
  op_counts.each do |op_count|
    alert_counts.each do |alert_count|
      alert_types.each do |alert_type|
        run_experiment(exp_type, op_count, alert_count, alert_type)
      end
    end
  end
end
