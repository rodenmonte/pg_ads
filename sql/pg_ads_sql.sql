RAISE NOTICE 'RUNNING PG_ADS SETUP SCRIPT';
create table if not exists pg_ads__tbl_parts_max_price_10 (
  max_price real
);
create or replace function pg_ads__fn_parts_max_price_10() returns trigger as $$
  declare
    agg_val real;
    
  begin
    select max(max_price) into agg_val from pg_ads__tbl_parts_max_price_10;
    
    -- If the the old `aggregate` is deleted
    IF (
  ((TG_OP = 'INSERT') AND agg_val IS NULL AND NEW.price IS NOT NULL)
) THEN

  INSERT INTO pg_ads__tbl_parts_max_price_10 (max_price) VALUES (NEW.price);
ELSIF (
  ((TG_OP = 'DELETE') AND OLD.price = agg_val) OR
  ((TG_OP = 'UPDATE') AND NEW.price < OLD.price)
) THEN
  UPDATE pg_ads__tbl_parts_max_price_10 SET max_price = (
    select max(price) FROM parts
  )
  WHERE max_price = agg_val;
ELSIF (
  ((TG_OP = 'UPDATE') AND NEW.price > OLD.price) OR
  ((TG_OP = 'INSERT') AND NEW.price > agg_val)
) THEN
  UPDATE pg_ads__tbl_parts_max_price_10
  SET max_price = NEW.price
  WHERE max_price = agg_val;
END IF;
select max(max_price) into agg_val from pg_ads__tbl_parts_max_price_10;
IF (
  agg_val > 10
) THEN
  RAISE NOTICE 'ALERT! MAX VALUE % VIOLATES MAX THRESHOLD 10', agg_val;
END IF;

    return null;
  end;
  $$ language plpgsql;
CREATE OR REPLACE TRIGGER pg_ads__trigger_parts_max_price_10_ins AFTER INSERT ON parts
FOR EACH ROW EXECUTE PROCEDURE pg_ads__fn_parts_max_price_10();
CREATE OR REPLACE TRIGGER pg_ads__trigger_parts_max_price_10_upd AFTER UPDATE ON parts
FOR EACH ROW EXECUTE PROCEDURE pg_ads__fn_parts_max_price_10();
CREATE OR REPLACE TRIGGER pg_ads__trigger_parts_max_price_10_del AFTER DELETE ON parts
FOR EACH ROW EXECUTE PROCEDURE pg_ads__fn_parts_max_price_10();
RAISE NOTICE 'RUNNING PG_ADS SETUP SCRIPT';
create table if not exists pg_ads__tbl_parts_min_price_0_1 (
  min_price real
);
create or replace function pg_ads__fn_parts_min_price_0_1() returns trigger as $$
  declare
    agg_val real;
    
  begin
    select min(min_price) into agg_val from pg_ads__tbl_parts_min_price_0_1;
    
    -- If the the old `aggregate` is deleted
    IF (
  ((TG_OP = 'INSERT') AND agg_val IS NULL AND NEW.price IS NOT NULL)
) THEN

  INSERT INTO pg_ads__tbl_parts_min_price_0_1 (min_price) VALUES (NEW.price);
ELSIF (
  ((TG_OP = 'DELETE') AND OLD.price = agg_val) OR
  ((TG_OP = 'UPDATE') AND NEW.price > OLD.price)
) THEN
  UPDATE pg_ads__tbl_parts_min_price_0_1 SET min_price = (
    select min(price) FROM parts
  )
  WHERE min_price = agg_val;
ELSIF (
  ((TG_OP = 'UPDATE') AND NEW.price < OLD.price) OR
  ((TG_OP = 'INSERT') AND NEW.price < agg_val)
) THEN
  UPDATE pg_ads__tbl_parts_min_price_0_1
  SET min_price = NEW.price
  WHERE min_price = agg_val;
END IF;
select min(min_price) into agg_val from pg_ads__tbl_parts_min_price_0_1;
IF (
  agg_val < 0.1
) THEN
  RAISE NOTICE 'ALERT! MIN VALUE % VIOLATES MIN THRESHOLD 0.1', agg_val;
END IF;

    return null;
  end;
  $$ language plpgsql;
CREATE OR REPLACE TRIGGER pg_ads__trigger_parts_min_price_0_1_ins AFTER INSERT ON parts
FOR EACH ROW EXECUTE PROCEDURE pg_ads__fn_parts_min_price_0_1();
CREATE OR REPLACE TRIGGER pg_ads__trigger_parts_min_price_0_1_upd AFTER UPDATE ON parts
FOR EACH ROW EXECUTE PROCEDURE pg_ads__fn_parts_min_price_0_1();
CREATE OR REPLACE TRIGGER pg_ads__trigger_parts_min_price_0_1_del AFTER DELETE ON parts
FOR EACH ROW EXECUTE PROCEDURE pg_ads__fn_parts_min_price_0_1();
RAISE NOTICE 'RUNNING PG_ADS SETUP SCRIPT';
create table if not exists pg_ads__tbl_parts_avg_price_0_1__10 (
  avg_price real, nb_rows int
);
create or replace function pg_ads__fn_parts_avg_price_0_1__10() returns trigger as $$
  declare
    agg_val real;
    nb_rows_fn int;
  begin
    select avg(avg_price) into agg_val from pg_ads__tbl_parts_avg_price_0_1__10;
    select nb_rows into nb_rows_fn from pg_ads__tbl_parts_avg_price_0_1__10;
    -- If the the old `aggregate` is deleted
    IF (
  (TG_OP = 'INSERT' AND agg_val IS NULL)
) THEN
  INSERT INTO pg_ads__tbl_parts_avg_price_0_1__10 (avg_price, nb_rows) VALUES (NEW.price, 1);
ELSIF (
  (TG_OP = 'INSERT')
) THEN
  UPDATE pg_ads__tbl_parts_avg_price_0_1__10 SET avg_price = (
    agg_val + ((NEW.price - agg_val) / (nb_rows_fn + 1))
  );
  UPDATE pg_ads__tbl_parts_avg_price_0_1__10 SET nb_rows = (nb_rows_fn + 1);
ELSIF (
  (TG_OP = 'DELETE')
) THEN
  IF (nb_rows_fn = 1) THEN
    TRUNCATE pg_ads__tbl_parts_avg_price_0_1__10;
  ELSE
    UPDATE pg_ads__tbl_parts_avg_price_0_1__10 SET avg_price = (
      ((agg_val * nb_rows_fn) - OLD.price) / (nb_rows_fn - 1)
    );
  END IF;
  UPDATE pg_ads__tbl_parts_avg_price_0_1__10 SET nb_rows = (nb_rows_fn - 1);
ELSIF (
  (TG_OP = 'UPDATE')
) THEN
  UPDATE pg_ads__tbl_parts_avg_price_0_1__10 SET avg_price = (
    ((agg_val * nb_rows_fn) - OLD.price) / (nb_rows_fn - 1)
  );
  UPDATE pg_ads__tbl_parts_avg_price_0_1__10 SET nb_rows = (nb_rows_fn + 1);
  -- could replace this select with better math, but.....
  select avg(avg_price) into agg_val from pg_ads__tbl_parts_avg_price_0_1__10;
  select nb_rows into nb_rows_fn from pg_ads__tbl_parts_avg_price_0_1__10;

  UPDATE pg_ads__tbl_parts_avg_price_0_1__10 SET avg_price = (
    agg_val + ((NEW.price - agg_val) / nb_rows_fn)
  );
  UPDATE pg_ads__tbl_parts_avg_price_0_1__10 SET nb_rows = (nb_rows_fn - 1);
END IF;
select avg(avg_price) into agg_val from pg_ads__tbl_parts_avg_price_0_1__10;
IF (
  agg_val > 10 OR
  agg_val < 0.1
) THEN
  RAISE NOTICE 'ALERT! AVERAGE % VIOLATES AVG THRESHOLD RANGE 0.1 TO 10', NEW.price;
END IF;

    return null;
  end;
  $$ language plpgsql;
CREATE OR REPLACE TRIGGER pg_ads__trigger_parts_avg_price_0_1__10_ins AFTER INSERT ON parts
FOR EACH ROW EXECUTE PROCEDURE pg_ads__fn_parts_avg_price_0_1__10();
CREATE OR REPLACE TRIGGER pg_ads__trigger_parts_avg_price_0_1__10_upd AFTER UPDATE ON parts
FOR EACH ROW EXECUTE PROCEDURE pg_ads__fn_parts_avg_price_0_1__10();
CREATE OR REPLACE TRIGGER pg_ads__trigger_parts_avg_price_0_1__10_del AFTER DELETE ON parts
FOR EACH ROW EXECUTE PROCEDURE pg_ads__fn_parts_avg_price_0_1__10();
