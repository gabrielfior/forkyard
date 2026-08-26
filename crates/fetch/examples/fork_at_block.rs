//! Manual verification for `forkyard_fetch::fork_at`: fork a specific
//! historical block and confirm the returned `BlockEnv` actually reports
//! that block number, not latest. Run with:
//!
//!     RPC_URL=... cargo run -p forkyard-fetch --example fork_at_block -- 20000000

use revm::primitives::U256;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let rpc_url = std::env::var("RPC_URL").expect("set RPC_URL");
    let requested: u64 = std::env::args()
        .nth(1)
        .expect("usage: fork_at_block <block_number>")
        .parse()
        .expect("block_number must be a u64");

    let (_fork, block_env) = forkyard_fetch::fork_at(&rpc_url, requested).await?;
    assert_eq!(
        block_env.number,
        U256::from(requested),
        "fork_at pinned to the wrong block"
    );
    println!("OK: fork_at({requested}) pinned to block {}", block_env.number);
    Ok(())
}
