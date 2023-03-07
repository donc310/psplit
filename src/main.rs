use psplit::{PSplit};

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Absolute Path to configuration file
    #[arg(short, long, value_name = "FILE", default_value_t = String::from("/usr/cvapps/pipes//config_splitter.ini"))]
    config: String,

    /// Log level
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Auto reload on config change
    #[arg(short, long)]
    reload: bool,
}

fn run_with_reload(_cli: &Cli) {
    todo!()
}

fn run(cli: &Cli) {
    let mut splitter = PSplit::new();
    splitter.config_from_file(&cli.config).start()
}

fn main() {
    let cli = Cli::parse();
    if cli.reload {
        run_with_reload(&cli)
    } else {
        run(&cli)
    }
}
