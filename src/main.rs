use anyhow::Result;

fn main() -> Result<()> {
    let message = torrust_lib::run()?;
    println!("{}", message);
    Ok(())
}
