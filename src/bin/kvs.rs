use clap::{App, AppSettings, Arg, SubCommand};

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
        .get_matches();

    std::process::exit(match handle_args(&matches) {
        Ok(_) => 0,
        Err(err) => {
            eprintln!("error: {:?}", err);
            1
        }
    })
}

fn handle_args(matches: &clap::ArgMatches) -> Result<(), String> {
    if let Some(matches) = matches.subcommand_matches("get") {
        let key = matches.value_of("KEY").unwrap();
        return Err(format!(
            "unimplemented, but would have looked up key {}",
            key
        ));
    }

    if let Some(matches) = matches.subcommand_matches("set") {
        let key = matches.value_of("KEY").unwrap();
        let value = matches.value_of("VALUE").unwrap();
        return Err(format!(
            "unimplemented, but would have set key {} to {}",
            key, value
        ));
    }

    if let Some(matches) = matches.subcommand_matches("rm") {
        let key = matches.value_of("KEY").unwrap();
        return Err(format!("unimplemented, but would have removed key {}", key));
    }

    Ok(())
}
