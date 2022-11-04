require 'yaml'

<<~BLORG
Notes and psuedocode:
TODO: Use statement triggers instead of row triggers

* Use calls fn
* Read in YAML file
* Convert YAML file into tables, functions, and triggers (deterministically? In a transaction?)
-- Do this part later
* Create pg_ads_triggers table if doesn't exist
* Write from YAML file here
* Delete rows in pg_ads_triggers and pg_triggers that don't correspond to YAML entries
-- End later
*
*


create table if not exists parts (
    id serial,
    price int
);

-- TODO: Put pg_ads tables into their own schema
create table if not exists pg_ads__tbl_parts_max_price (
    max_price int
);

insert into parts(price) values (3);


-- Just testing fns
create or replace function testy() returns void as $$
declare
  agg_val int;
begin
  select max(price) into agg_val from parts;
  if 3 = agg_val then
    raise notice 'IT BIG';
  end if;
end;
$$ language plpgsql;


create or replace function pg_ads__fn_parts_max_price() returns trigger as $$
declare
  agg_val int; -- TODO: Change type based off YAML
begin
  -- TODO: Replace agg op, column, and table
  select max(max_price) into agg_val from pg_ads__tbl_parts_max_price;
  -- Unhappy case, the old max is deleted
  IF (
      ((TG_OP = 'DELETE") and OLD.price = agg_val) or
      ((TG_OP = 'UPDATE') and OLD.price > NEW.price)
     ) THEN
    update pg_ads__tbl_parts_max_price set max_price = (select max(price) from parts) where max_price = agg_val;
  elsif (
      ((TG_OP = 'UPDATE") and OLD.price < NEW.price) or
      ((TG_OP = 'INSERT') and NEW.price > agg_val)
     ) THEN
     update pg_ads__tbl_parts_max_price set max_price = NEW.price where max_price = agg_val;
  end if;
  return null;
end;
$$ language plpgsql;





create or replace function pg_ads__fn_parts_max_price() returns trigger as $$
begin
  IF (TG_OP = 'DELETE") THEN
    OLD.max_price = ANY (
    insert into pg_ads__tbl_parts_max_price select 'd'
end;
$$ language plpgsql;
BLORG

class Anomaly
  SUPPORTED_AGGREGATES = ['max', 'min', 'avg']

  attr_reader :name, :table, :aggregate, :column, :column_type

  def initialize(hsh)
    @name = hsh['name']
    @table = hsh['parts']
    @aggregate = assign_aggregate hsh['aggregate']
    @column = hsh['column']
    # TODO: Derive type, don't make the user specify
    @column_type = hsh['column_type']
  end

  def to_table
    columns = case aggregate
      when 'max', 'min'
        { column_name => column_type }
      when 'avg'
        { column_name => column_type, 'n_records' => 'int' }
      else
        raise 'WTF'
      end

    <<~SQL
    create table if not exists #{internal_table} (
      #{columns.map { |name, type| [name, type].join(" ") }.join(", ")}
    );
    SQL
  end

  def to_trigger_fn
    <<~SQL
    create or replace function #{trigger_fn_name}() returns trigger as $$
      declare
        agg_val #{column_type};
      begin
        select #{aggregate}(#{column_name}) into agg_val from #{internal_table};
        -- Unhappy case, the old `aggregate` is deleted
        #{self.send(:"#{aggregate}_trigger")}
        return null;
      end;
      $$ language plpgsql;
    SQL
  end

  def to_trigger
    <<~SQL
      CREATE TRIGGER #{trigger_name} AFTER INSERT ON #{table}
      FOR EACH ROW EXECUTE PROCEDURE #{trigger_fn_name}()
    SQL
  end

  private

  def trigger_name
    "pg_ads__trigger_#{table}_#{column_name}"
  end

  # TODO: Use a pg_ads schema instead of pg_ads__ prefixes
  def trigger_fn_name
    "pg_ads__fn_#{table}_#{column_name}"
  end

  def internal_table
    "pg_ads__tbl_#{table}_#{column_name}"
  end

  def column_name
    "#{aggregate}_#{column}"
  end

  def assign_aggregate(potential_aggregate)
    unless SUPPORTED_AGGREGATES.include? potential_aggregate
      raise "Unsupported aggregate #{potential_aggregate}"
    end

    potential_aggregate
  end

  def min_max_trigger(new_column_happy_case)
    new_column_sad_case = case new_column_happy_case
      when ">"
        "<"
      when "<"
        ">"
      else
        raise "WTF"
      end

    <<~SQL
    IF (
      ((TG_OP = 'DELETE') AND OLD.#{column} = agg_val) OR
      ((TG_OP = 'UPDATE') AND NEW.#{column} #{new_column_sad_case} OLD.#{column})
    ) THEN
      UPDATE #{internal_table} SET #{column_name} = (
        select #{aggregate}(#{column}) FROM #{table}
      )
      WHERE #{column_name} = agg_val;
    ELSIF (
      ((TG_OP = 'UPDATE') AND NEW.#{column} #{new_column_happy_case} OLD.#{column}) OR
      ((TG_OP = 'INSERT') AND NEW.#{column} #{new_column_happy_case} agg_val)
    ) THEN
      UPDATE #{internal_table}
      SET #{column_name} = NEW.#{column}
      WHERE #{column_name} = agg_val;
    END IF;
    SQL
  end

  def max_trigger
    min_max_trigger ">"
  end

  def min_trigger
    min_max_trigger "<"
  end

  def avg_trigger
    <<~SQL
    IF (
      ((TG_OP = 'DELETE') AND OLD.#{column} = agg_val) OR
      ((TG_OP = 'UPDATE') AND NEW.#{column} #{'avg'} OLD.#{column})
    ) THEN
      UPDATE #{internal_table} SET #{column_name} = (
        select #{aggregate}(#{column}) FROM #{table}
      )
      WHERE #{column_name} = agg_val;
    ELSIF (
      ((TG_OP = 'UPDATE') AND NEW.#{column} #{'avg'} OLD.#{column}) OR
      ((TG_OP = 'INSERT') AND NEW.#{column} #{'avg'} agg_val)
    ) THEN
      UPDATE #{internal_table}
      SET #{column_name} = NEW.#{column}
      WHERE #{column_name} = agg_val;
    END IF;
    SQL
  end
end

def read_yaml
  YAML.load_file('test.yaml')
end

# yml is an array of hashes
def yaml_to_anomalies(yml = read_yaml)
  yml.map { |anomaly| Anomaly.new(anomaly) }
end

yaml_to_anomalies.each do |anom|
  puts anom.to_table
  puts anom.to_trigger_fn
  puts anom.to_trigger
end
