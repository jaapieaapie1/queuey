//! Proof that the type-safety claim is real: mixing two queue sets is a compile
//! error, not a runtime surprise.
//!
//! Every case here depends on `queuey` alone, so it also covers the
//! derive macros resolving their paths through the facade.
//!
//! Regenerate the expected output with:
//! `TRYBUILD=overwrite cargo test -p queuey --test compile_fail`

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.pass("tests/compile_fail/pass/*.rs");
    t.compile_fail("tests/compile_fail/*.rs");
}
