#!/usr/bin/env ruby

require 'yaml'

class Anomaly
  SUPPORTED_AGGREGATES = ['max', 'min', 'avg']

  attr_reader :name, :table, :aggregate, :column, :column_type

  def initialize(hsh)
    @name = hsh['name']
    @table = hsh['table']
    @aggregate = assign_aggregate hsh['aggregate']
    @column = hsh['column']
    # TODO: Derive type, don't make the user specify
    @column_type = hsh['column_type']
    if aggregate == 'avg'
      @threshold_min = hsh['threshold_min']
      @threshold_max = hsh['threshold_max']
    else
      @threshold = hsh['threshold']
    end
  end

  def to_table
    columns = case aggregate
      when 'max', 'min'
        { column_name => column_type }
      when 'avg'
        { column_name => column_type, 'nb_rows' => 'int' }
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
        #{
          if aggregate == 'avg'
            "nb_rows_fn int;"
          end
        }
      begin
        select #{aggregate}(#{column_name}) into agg_val from #{internal_table};
        #{
          if aggregate == 'avg'
            "select nb_rows into nb_rows_fn from #{internal_table};"
          end
        }
        -- If the the old `aggregate` is deleted
        #{self.send(:"#{aggregate}_trigger")}
        return null;
      end;
      $$ language plpgsql;
    SQL
  end

  def to_trigger
    <<~SQL
      CREATE OR REPLACE TRIGGER #{trigger_name}_ins AFTER INSERT ON #{table}
      FOR EACH ROW EXECUTE PROCEDURE #{trigger_fn_name}();
      CREATE OR REPLACE TRIGGER #{trigger_name}_upd AFTER UPDATE ON #{table}
      FOR EACH ROW EXECUTE PROCEDURE #{trigger_fn_name}();
      CREATE OR REPLACE TRIGGER #{trigger_name}_del AFTER DELETE ON #{table}
      FOR EACH ROW EXECUTE PROCEDURE #{trigger_fn_name}();
    SQL
  end

  private

  def trigger_name
    "pg_ads__trigger_#{table}_#{column_name}_#{threshold_id}"
  end

  # TODO: Use a pg_ads schema instead of pg_ads__ prefixes
  def trigger_fn_name
    "pg_ads__fn_#{table}_#{column_name}_#{threshold_id}"
  end

  def internal_table
    "pg_ads__tbl_#{table}_#{column_name}_#{threshold_id}"
  end

  def threshold_id
    if aggregate == 'avg'
      "#{@threshold_min}__#{@threshold_max}".gsub(".", "_")
    else
      "#{@threshold}".gsub(".", "_")
    end
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
      ((TG_OP = 'INSERT') AND agg_val IS NULL AND NEW.#{column} IS NOT NULL)
    ) THEN
  
      INSERT INTO #{internal_table} (#{column_name}) VALUES (NEW.#{column});
    ELSIF (
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
    select #{aggregate}(#{column_name}) into agg_val from #{internal_table};
    IF (
      agg_val #{new_column_happy_case} #{@threshold}
    ) THEN
      RAISE NOTICE 'ALERT! #{aggregate.upcase} VALUE % VIOLATES #{aggregate.upcase} THRESHOLD #{@threshold}', agg_val;
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
      (TG_OP = 'INSERT' AND agg_val IS NULL)
    ) THEN
      INSERT INTO #{internal_table} (#{column_name}, nb_rows) VALUES (NEW.#{column}, 1);
    ELSIF (
      (TG_OP = 'INSERT')
    ) THEN
      UPDATE #{internal_table} SET #{column_name} = (
        agg_val + ((NEW.#{column} - agg_val) / (nb_rows_fn + 1))
      );
      UPDATE #{internal_table} SET nb_rows = (nb_rows_fn + 1);
    ELSIF (
      (TG_OP = 'DELETE')
    ) THEN
      IF (nb_rows_fn = 1) THEN
        TRUNCATE #{internal_table};
      ELSE
        UPDATE #{internal_table} SET #{column_name} = (
          ((agg_val * nb_rows_fn) - OLD.#{column}) / (nb_rows_fn - 1)
        );
      END IF;
      UPDATE #{internal_table} SET nb_rows = (nb_rows_fn - 1);
    ELSIF (
      (TG_OP = 'UPDATE')
    ) THEN
      UPDATE #{internal_table} SET #{column_name} = (
        ((agg_val * nb_rows_fn) - OLD.#{column}) / (nb_rows_fn - 1)
      );
      UPDATE #{internal_table} SET nb_rows = (nb_rows_fn + 1);
      -- could replace this select with better math, but.....
      select #{aggregate}(#{column_name}) into agg_val from #{internal_table};
      select nb_rows into nb_rows_fn from #{internal_table};

      UPDATE #{internal_table} SET #{column_name} = (
        agg_val + ((NEW.#{column} - agg_val) / nb_rows_fn)
      );
      UPDATE #{internal_table} SET nb_rows = (nb_rows_fn - 1);
    END IF;
    select #{aggregate}(#{column_name}) into agg_val from #{internal_table};
    IF (
      agg_val > #{@threshold_max} OR
      agg_val < #{@threshold_min}
    ) THEN
      RAISE NOTICE 'ALERT! AVERAGE % VIOLATES AVG THRESHOLD RANGE #{@threshold_min} TO #{@threshold_max}', NEW.#{column};
    END IF;
    SQL
  end
end

def read_yaml
  f = ARGV[0] || 'test.yaml'
  YAML.load_file f
rescue
  puts "File #{f} couldn't be found, exiting..."
  exit 1
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
