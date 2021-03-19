#[cfg(feature = "in_memory")]
pub use in_memory::*;
#[cfg(feature = "persistent")]
pub use persistent::*;

pub fn test_feature() {
    #[cfg(feature = "persistent")]
    println!("STORAGE TEST persistent");
    #[cfg(feature = "in_memory")]
    println!("STORAGE TEST in_memory");
}
