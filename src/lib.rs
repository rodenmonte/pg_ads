use std::fs;
use yaml_rust::{YamlLoader};
use yaml_rust::Yaml;
use yaml_rust::yaml::{Hash, Array};
use pgx::prelude::*;

//impl IntoIterator for Yaml {
//    type Item = Yaml;
//    type IntoIter = YamlIter;
//    fn into_iter(self) -> Self::IntoIter {
//        YamlIter {
//            yaml: self.into_vec().unwrap_or_else(Vec::new).into_iter(),
//        }
//    }
//}
// impl Clone for Yaml {};

pgx::pg_module_magic!();

#[pg_extern]
fn hello_pg_ads() -> &'static str {
    "Hello, pg_ads"
}


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

    #[pg_test]
    fn test_hello_pg_ads() {
        assert_eq!("Hello, pg_ads", crate::hello_pg_ads());
    }

}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
