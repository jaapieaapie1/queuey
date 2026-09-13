//! Compile-fail (and one compile-pass) coverage for the derive macros.
//!
//! Regenerate the expected output with:
//! `TRYBUILD=overwrite cargo test -p queuey-macros --test ui`

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass/*.rs");
    t.compile_fail("tests/ui/*.rs");
}
