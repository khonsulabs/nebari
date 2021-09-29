use chrono::Utc;
use devx_cmd::read;
use khonsu_tools::{
    anyhow,
    code_coverage::{self, CodeCoverage},
};
use structopt::StructOpt;

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
    let now = Utc::now();
    let git_rev = read!("git", "rev-parse", "HEAD")?;
    let git_rev = git_rev.trim();

    let overview = overview.replace("TIMESTAMP", &now.to_rfc2822());
    let overview = overview.replace("GITREV", git_rev);

    std::fs::write("target/criterion/index.html", &overview)?;

    Ok(())
}
