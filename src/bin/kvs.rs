use clap::ArgMatches;

#[macro_use]
extern crate clap;
use clap::{arg, App, Command};
use kvs::KvStore;

fn arg_parse() -> ArgMatches {
    App::new(crate_name!())
        .author(crate_authors!())
        .about(crate_description!())
        .version(crate_version!())
        .arg_required_else_help(true)
        .subcommands(vec![
            Command::new("get").arg(arg!(<key>)),
            Command::new("set").args(vec![arg!(<key>), arg!(<value>)]),
            Command::new("rm").arg(arg!(<key>)),
        ])
        .get_matches()
}

fn unwrap_required_arg(matches: &ArgMatches, key: &str) -> String {
    matches.value_of(key).unwrap().to_owned()
}
fn handle_command(matches: &ArgMatches, store: &mut KvStore) {
    match matches.subcommand() {
        Some(("get", sub_m)) => {
            let key = unwrap_required_arg(sub_m, "key");
            println!("{}", store.get(key).unwrap().expect("no key found"));
        }
        Some(("set", sub_m)) => {
            let key = unwrap_required_arg(sub_m, "key");
            let value = unwrap_required_arg(sub_m, "value");
            store.set(key, value).unwrap();
        }
        Some(("rm", sub_m)) => {
            let key = unwrap_required_arg(sub_m, "key");
            store.remove(key).unwrap();
        }
        _ => panic!("command not found"),
    }
}

fn main() {
    let matches = arg_parse();
    let mut store = KvStore::open("logs.log").unwrap();
    handle_command(&matches, &mut store);
}
