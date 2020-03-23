use http::{Request, Response};

fn main() {
    println!("Hello, world!");
    let request = Request::builder()
        .uri("https://www.rust-lang.org/")
        .header("User-Agent", "awesome/1.0")
        .body(())
        .unwrap();
    println!("{:?}", request);
}
