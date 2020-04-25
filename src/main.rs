mod types;
mod engine;

use crate::engine::Engine;

fn main() {
  let mut engine = Engine::default();
  engine.execute(
    "CREATE TABLE simple_table (\n\
      int_column INT,\n\
    );".to_owned()
  )
      .and_then(|_| engine.execute(format!("INSERT INTO simple_table VALUES ({});", 1)))
      .and_then(|_| engine.execute("SELECT int_column FROM simple_table WHERE int_column = 1;".to_owned()))
      .map(|_| println!("Hello, world!"))
      .unwrap_or_else(|e| eprintln!("Something went wrong {:?}", e));
}
