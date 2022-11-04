use std::fs;
use yaml_rust::{YamlLoader};
use yaml_rust::Yaml;
use yaml_rust::yaml::{Hash, Array};
use pgx::prelude::*;

pgx::pg_module_magic!();

// TODO: Turn config into a list of Anomalies or AnomalyRules
// Then, in a subsequent function, turn those into triggers
// TODO: I think a second table to track triggers, `pg_ads_triggers`,
// should be used s.t. new and old triggers can be swapped when some fn. is run.
fn pg_ads_read_config() -> Vec<Yaml> {
    let contents = fs::read_to_string("/home/monter/code/pg_ads/test.yaml").expect("Couldn't read yaml file");
    let mut vec = Vec::new();
    let yml_wrapper = YamlLoader::load_from_str(&contents).unwrap();
    let yml = yml_wrapper.first().unwrap();
    yml.clone().into_iter().for_each(|entry| vec.push(entry));
    vec
}

#[pg_extern]
fn pg_ads_create_triggers() -> &'static str {
    pgx::info!("GOT HERE HERE");
    let triggers = pg_ads_read_config();
    // triggers.iter().for_each(|(k, v)| println!("{k}: {v}"));
    triggers.iter().for_each(|y| pgx::info!("{:?}", y));
    "COMPLETE"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::prelude::*;
    use pgx::Spi;

    #[pg_test]
    fn test_insert() {
        // TODO: Verify selecting from parts max table looks good
        Spi::run(
            r#"insert into parts(price) values (3);"#,
        );
    }

}

#[cfg(test)]
pub mod pg_test {
    use pgx::Spi;

    pub fn setup(_options: Vec<&str>) {
        Spi::run(
            r#"
              create table if not exists parts (
                id serial,
                price int
              );
            "#,
        );
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}

