use devx_cmd::read;
use khonsu_tools::{
    anyhow,
    code_coverage::{self, CodeCoverage},
};
use structopt::StructOpt;
use sysinfo::{RefreshKind, System, SystemExt};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

#[derive(StructOpt, Debug)]
pub enum Commands {
    GenerateCodeCoverageReport {
        #[structopt(long = "install-dependencies")]
        install_dependencies: bool,
    },
    GenerateBenchmarkOverview,
}

fn main() -> anyhow::Result<()> {
    let command = Commands::from_args();
    match command {
        Commands::GenerateBenchmarkOverview => generate_benchmark_overview(),
        Commands::GenerateCodeCoverageReport {
            install_dependencies,
        } => CodeCoverage::<CoverageConfig>::execute(install_dependencies),
    }
}

struct CoverageConfig;

impl code_coverage::Config for CoverageConfig {}

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
