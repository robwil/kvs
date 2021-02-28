use anyhow::Context;
use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvStore, Result};
use std::io;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("get").about("get value by KEY").arg(
                Arg::with_name("KEY")
                    .required(true)
                    .index(1)
                    .help("the key to look up"),
            ),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("set KEY to VALUE")
                .arg(
                    Arg::with_name("KEY")
                        .required(true)
                        .index(1)
                        .help("the key to set"),
                )
                .arg(
                    Arg::with_name("VALUE")
                        .required(true)
                        .index(2)
                        .help("the value to set KEY to"),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("remove value by KEY")
                .arg(
                    Arg::with_name("KEY")
                        .required(true)
                        .index(1)
                        .help("the key to remove"),
                ),
        )
        .subcommand(
            SubCommand::with_name("interactive")
                .about("Run in interactive mode, allowing multiple commands"),
        )
        .get_matches();

    std::process::exit(match handle_args(&matches) {
        Ok(_) => 0,
        Err(err) => {
            println!("error: {:?}", err);
            1
        }
    })
}

fn handle_args(matches: &clap::ArgMatches) -> Result<()> {
    let mut kv_store = KvStore::open(".")?;
    if let Some(matches) = matches.subcommand_matches("get") {
        let key = matches.value_of("KEY").context("Getting KEY value")?;
        handle_get(&mut kv_store, key)?;
    }

    if let Some(matches) = matches.subcommand_matches("set") {
        let key = matches.value_of("KEY").context("Getting KEY value")?;
        let value = matches.value_of("VALUE").context("Getting VALUE value")?;
        handle_set(&mut kv_store, key, value)?;
    }

    if let Some(matches) = matches.subcommand_matches("rm") {
        let key = matches.value_of("KEY").context("Getting KEY value")?;
        handle_rm(&mut kv_store, key)?;
    }

    if matches.subcommand_matches("interactive").is_some() {
        println!("Welcome to interactive mode. Type \"exit\" to end.");
        loop {
            let stdin = io::stdin(); // We get `Stdin` here.
            let mut buffer = String::new();
            stdin.read_line(&mut buffer)?;
            let split: Vec<_> = buffer.split(' ')
                .map(|str| str.trim())
                .collect();
            match split.get(0).cloned() {
                None => break,
                Some("") => break,
                Some("exit") => break,
                // TODO: better error handling for missing args
                Some("get") => {
                    let key = split.get(1).context("Getting KEY value")?;
                    handle_get(&mut kv_store, key)?;
                },
                Some("set") => {
                    let key = split.get(1).context("Getting KEY value")?;
                    let value = split.get(2).context("Getting VALUE value")?;
                    handle_set(&mut kv_store, key, value)?;
                },
                Some("rm") => {
                    let key = split.get(1).context("Getting KEY value")?;
                    handle_rm(&mut kv_store, key)?;
                },
                Some(_) => println!("unknown command"),
            }
        }
    }

    Ok(())
}

fn handle_get(kv_store: &mut KvStore, key: &str) -> Result<()> {
    if let Some(value) = kv_store.get(key.to_owned())? {
        println!("{}", value);
    } else {
        // print error but still exit ok
        println!("Key not found");
    }
    Ok(())
}

fn handle_set(kv_store: &mut KvStore, key: &str, value: &str) -> Result<()> {
    kv_store.set(key.to_owned(), value.to_owned())?;
    Ok(())
}

fn handle_rm(kv_store: &mut KvStore, key: &str) -> Result<()> {
    kv_store.remove(key.to_owned())?;
    Ok(())
}