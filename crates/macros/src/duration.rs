//! Macro-time parsing of duration literals such as `"500ms"`, `"2m"` or `"30"`.
//!
//! Durations are resolved to whole milliseconds while the macro runs, so the
//! generated code only ever contains `::core::time::Duration::from_millis(N)`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::LitStr;

const MILLIS_PER_SECOND: u64 = 1_000;
const MILLIS_PER_MINUTE: u64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: u64 = 60 * MILLIS_PER_MINUTE;
const MILLIS_PER_DAY: u64 = 24 * MILLIS_PER_HOUR;

/// Parse a duration literal into whole milliseconds.
///
/// Accepted shapes: `<integer><unit>` where `unit` is one of `ms`, `s`, `m`,
/// `h`, `d`. Whitespace around the value and between value and unit is
/// ignored. A bare integer is interpreted as seconds.
pub(crate) fn parse_millis(input: &str) -> Result<u64, String> {
    let trimmed = input.trim();
    let digit_end = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, rest) = trimmed.split_at(digit_end);
    if digits.is_empty() {
        return Err(invalid(input));
    }

    let unit = rest.trim_start();
    let factor = match unit {
        "" | "s" => MILLIS_PER_SECOND,
        "ms" => 1,
        "m" => MILLIS_PER_MINUTE,
        "h" => MILLIS_PER_HOUR,
        "d" => MILLIS_PER_DAY,
        _ => return Err(invalid(input)),
    };

    let value: u64 = digits.parse().map_err(|_| out_of_range(input))?;
    value.checked_mul(factor).ok_or_else(|| out_of_range(input))
}

/// Parse a duration string literal, reporting failures spanned at the literal.
///
/// `key` is the attribute key the literal belongs to; it is only used in error
/// messages. Zero is rejected: every duration in this grammar is a delay or a
/// TTL, and both are meaningless (or actively destructive) at zero.
pub(crate) fn parse_lit(lit: &LitStr, key: &str) -> syn::Result<u64> {
    let millis =
        parse_millis(&lit.value()).map_err(|message| syn::Error::new(lit.span(), message))?;
    if millis == 0 {
        return Err(syn::Error::new(lit.span(), zero(key)));
    }
    Ok(millis)
}

/// Render `millis` as a `::core::time::Duration` expression.
pub(crate) fn to_tokens(millis: u64) -> TokenStream {
    quote!(::core::time::Duration::from_millis(#millis))
}

fn invalid(input: &str) -> String {
    format!(
        "invalid duration `{input}`: expected an integer with an optional unit \
         (`ms`, `s`, `m`, `h`, `d`), for example \"500ms\", \"30s\" or \"2m\""
    )
}

fn out_of_range(input: &str) -> String {
    format!("duration `{input}` does not fit in a `u64` number of milliseconds")
}

fn zero(key: &str) -> String {
    let hint = if key == "message_ttl" {
        "a zero TTL discards every message the moment it is published"
    } else {
        "express \"no backoff\" as `backoff = \"none\"` instead"
    };
    format!("`{key}` must be greater than zero: {hint}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_unit() {
        assert_eq!(parse_millis("500ms"), Ok(500));
        assert_eq!(parse_millis("1s"), Ok(1_000));
        assert_eq!(parse_millis("2m"), Ok(120_000));
        assert_eq!(parse_millis("1h"), Ok(3_600_000));
        assert_eq!(parse_millis("1d"), Ok(86_400_000));
    }

    #[test]
    fn bare_integer_is_seconds() {
        assert_eq!(parse_millis("30"), Ok(30_000));
        assert_eq!(parse_millis("0"), Ok(0));
    }

    #[test]
    fn whitespace_is_ignored() {
        assert_eq!(parse_millis("  5 s "), Ok(5_000));
        assert_eq!(parse_millis("5 ms"), Ok(5));
        assert_eq!(parse_millis("\t300\tms\n"), Ok(300));
    }

    #[test]
    fn rejects_bad_input() {
        for bad in [
            "", "   ", "s", "abc", "1x", "1.5s", "-1s", "1sm", "1 m s", "ms1", "+2s",
        ] {
            assert!(
                parse_millis(bad).is_err(),
                "expected `{bad}` to be rejected"
            );
        }
    }

    #[test]
    fn error_message_mentions_the_input_and_units() {
        let err = parse_millis("1x").unwrap_err();
        assert!(err.contains("`1x`"), "{err}");
        assert!(err.contains("`ms`"), "{err}");
    }

    #[test]
    fn rejects_overflow() {
        let err = parse_millis("18446744073709551615h").unwrap_err();
        assert!(err.contains("u64"), "{err}");
        let err = parse_millis("99999999999999999999999").unwrap_err();
        assert!(err.contains("u64"), "{err}");
    }

    #[test]
    fn renders_duration_tokens() {
        assert_eq!(
            to_tokens(1_500).to_string(),
            quote!(::core::time::Duration::from_millis(1500u64)).to_string()
        );
    }

    #[test]
    fn parse_lit_is_spanned() {
        let lit: LitStr = syn::parse_quote!("2m");
        assert_eq!(parse_lit(&lit, "max").unwrap(), 120_000);
        let bad: LitStr = syn::parse_quote!("nope");
        assert!(parse_lit(&bad, "max").is_err());
    }

    #[test]
    fn parse_lit_rejects_zero_durations() {
        for text in ["0", "0s", "0ms", " 0 ms "] {
            let lit = LitStr::new(text, proc_macro2::Span::call_site());
            let err = parse_lit(&lit, "delay").unwrap_err().to_string();
            assert!(err.contains("`delay` must be greater than zero"), "{err}");
            assert!(err.contains("backoff = \"none\""), "{err}");
        }
    }

    #[test]
    fn zero_message_ttl_explains_the_discard() {
        let lit: LitStr = syn::parse_quote!("0ms");
        let err = parse_lit(&lit, "message_ttl").unwrap_err().to_string();
        assert!(
            err.contains("`message_ttl` must be greater than zero"),
            "{err}"
        );
        assert!(err.contains("discards every message"), "{err}");
    }
}
