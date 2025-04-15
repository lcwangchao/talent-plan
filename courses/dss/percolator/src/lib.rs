#[allow(unused_imports)]
#[macro_use]
extern crate log;

// After you finish the implementation, `#[allow(unused)]` should be removed.
#[allow(dead_code, unused)]
mod client;
#[allow(unused)]
mod server;
mod service;
#[cfg(test)]
mod tests;
