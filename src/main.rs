use psplit::split_pipes;

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Absolute Path to configuration file
    #[arg(short, long, value_name = "FILE", default_value_t = String::from("/usr/cvapps/pipes/config_splitter.ini"))]
    config: String,

    /// Log level
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Auto reload on config change
    #[arg(short, long)]
    reload: bool,
}

fn run_with_reload(_cli: &Args) -> Result<(), std::io::Error> {
    todo!()
}

fn run(cli: &Args) -> Result<(), std::io::Error> {
    split_pipes(&cli.config)
}

fn main() -> Result<(), std::io::Error> {
    let cli = Args::parse();
    
    if cli.reload {
        run_with_reload(&cli)
    } else {
        run(&cli)
    }
}
