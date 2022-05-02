use devx_cmd::read;
use khonsu_tools::{
    publish,
    universal::{
        anyhow, audit,
        clap::{self, Parser},
        code_coverage,
    },
};
use sysinfo::{RefreshKind, System, SystemExt};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

#[derive(Parser, Debug)]
pub enum Commands {
    GenerateBenchmarkOverview,
    #[clap(flatten)]
    Tools(khonsu_tools::Commands),
}

fn main() -> anyhow::Result<()> {
    let command = Commands::parse();
    match command {
        Commands::GenerateBenchmarkOverview => generate_benchmark_overview(),
        Commands::Tools(command) => command.execute::<Config>(),
    }
}

fn generate_benchmark_overview() -> anyhow::Result<()> {
    let overview = std::fs::read_to_string("benchmarks/overview.html")?;
    let now = OffsetDateTime::now_utc();
    let git_rev = read!("git", "rev-parse", "HEAD")?;
    let git_rev = git_rev.trim();

    let overview = overview.replace("TIMESTAMP", &now.format(&Rfc3339).unwrap());
    let overview = overview.replace("GITREV", git_rev);
    let environment = match std::env::var("ENVIRONMENT") {
        Ok(environment) => environment,
        Err(_) => {
            let whoami = read!("whoami")?;
            let whoami = whoami.trim();
            let system = System::new_with_specifics(RefreshKind::new().with_cpu().with_memory());
            format!(
                "on {}'s machine with {} cores and {} GB of RAM",
                whoami,
                system
                    .physical_core_count()
                    .expect("unable to count processor cores"),
                system.total_memory() / 1024 / 1024,
            )
        }
    };
    let overview = overview.replace("ENVIRONMENT", &environment);

    std::fs::write("target/criterion/index.html", &overview)?;

    Ok(())
}

enum Config {}

impl khonsu_tools::Config for Config {
    type Publish = Self;

    type Universal = Self;
}

impl khonsu_tools::universal::Config for Config {
    type Audit = Self;

    type CodeCoverage = Self;
}

impl audit::Config for Config {
    fn args() -> Vec<String> {
        vec![
            String::from("--all-features"),
            String::from("--exclude=xtask"),
            String::from("--exclude=benchmarks"),
        ]
    }
}

impl publish::Config for Config {
    fn paths() -> Vec<String> {
        vec![String::from("nebari")]
    }
}

impl code_coverage::Config for Config {
    fn cargo_args() -> Vec<String> {
        vec![String::from("test"), String::from("--all-features")]
    }
}
