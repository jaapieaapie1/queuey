//! Grammar shared by `#[derive(Queues)]` and `#[derive(Job)]`.
//!
//! Everything here works on `syn` types only, so it can be unit tested without
//! expanding a macro.

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::meta::ParseNestedMeta;
use syn::parse::Parser as _;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, ExprUnary, Lit, LitFloat, LitStr, Path, UnOp};

use crate::duration;

/// Default `max_attempts` when `retry(...)` omits it.
pub(crate) const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Defaults mirroring `queuey_core::Backoff::exponential()`.
pub(crate) const DEFAULT_EXP_BASE_MS: u64 = 1_000;
pub(crate) const DEFAULT_EXP_FACTOR: f64 = 2.0;
pub(crate) const DEFAULT_EXP_MAX_MS: u64 = 300_000;
pub(crate) const DEFAULT_EXP_JITTER: bool = true;

/// A parsed value together with the span of the key it came from.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Keyed<T> {
    pub(crate) value: T,
    pub(crate) span: Span,
}

impl<T> Keyed<T> {
    fn new(value: T, span: Span) -> Self {
        Self { value, span }
    }
}

/// Record a key exactly once, rejecting repeats with a spanned error.
pub(crate) fn set_once<T>(
    slot: &mut Option<Keyed<T>>,
    meta: &ParseNestedMeta,
    value: T,
) -> syn::Result<()> {
    let span = meta.path.span();
    if slot.is_some() {
        return Err(syn::Error::new(
            span,
            format!("duplicate key `{}`", key_name(&meta.path)),
        ));
    }
    *slot = Some(Keyed::new(value, span));
    Ok(())
}

/// Human readable name of an attribute key.
pub(crate) fn key_name(path: &Path) -> String {
    match path.get_ident() {
        Some(ident) => ident.to_string(),
        None => quote!(#path).to_string().replace(' ', ""),
    }
}

/// `key = "..."`
pub(crate) fn lit_str(meta: &ParseNestedMeta) -> syn::Result<LitStr> {
    match meta.value()?.parse::<Lit>()? {
        Lit::Str(lit) => Ok(lit),
        other => Err(syn::Error::new(
            other.span(),
            format!("`{}` must be a string literal", key_name(&meta.path)),
        )),
    }
}

/// `key = true` / `key = false`
pub(crate) fn lit_bool(meta: &ParseNestedMeta) -> syn::Result<bool> {
    match meta.value()?.parse::<Lit>()? {
        Lit::Bool(lit) => Ok(lit.value),
        other => Err(syn::Error::new(
            other.span(),
            format!("`{}` must be `true` or `false`", key_name(&meta.path)),
        )),
    }
}

/// `key = 12` parsed into any integer type.
///
/// `range` describes the accepted values (for example `"between 1 and 65535"`)
/// and is used for every rejection, so a negative, fractional, non-numeric or
/// out-of-range literal all report the same, key-named message instead of
/// `syn`'s raw `invalid digit found in string`.
pub(crate) fn lit_int<T>(meta: &ParseNestedMeta, range: &str) -> syn::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let key = key_name(&meta.path);
    let lit = match meta.value()?.parse::<Lit>()? {
        Lit::Int(lit) => lit,
        other => {
            return Err(syn::Error::new(
                other.span(),
                format!("`{key}` must be an integer {range}"),
            ));
        }
    };
    lit.base10_parse::<T>()
        .map_err(|_| syn::Error::new(lit.span(), format!("`{key}` must be an integer {range}")))
}

/// Accepted range of `prefetch`, shared by the parser and its error message.
pub(crate) const PREFETCH_RANGE: &str = "between 1 and 65535";
/// Accepted range of `max_priority`; `0` is legal and means "not a priority queue".
pub(crate) const MAX_PRIORITY_RANGE: &str = "in 0..=255";
/// Accepted range of `max_attempts`.
pub(crate) const MAX_ATTEMPTS_RANGE: &str = "between 1 and 4294967295";

/// `key = 2.0`, also accepting `2` and negative literals.
pub(crate) fn lit_f64(meta: &ParseNestedMeta) -> syn::Result<f64> {
    let expr = meta.value()?.parse::<Expr>()?;
    let (lit, negative) = match &expr {
        Expr::Lit(ExprLit { lit, .. }) => (lit, false),
        Expr::Unary(ExprUnary {
            op: UnOp::Neg(_),
            expr,
            ..
        }) => match &**expr {
            Expr::Lit(ExprLit { lit, .. }) => (lit, true),
            _ => return Err(float_error(meta, expr.span())),
        },
        _ => return Err(float_error(meta, expr.span())),
    };
    let magnitude = match lit {
        Lit::Float(lit) => lit.base10_parse::<f64>()?,
        Lit::Int(lit) => lit.base10_parse::<f64>()?,
        other => return Err(float_error(meta, other.span())),
    };
    Ok(if negative { -magnitude } else { magnitude })
}

fn float_error(meta: &ParseNestedMeta, span: Span) -> syn::Error {
    syn::Error::new(
        span,
        format!(
            "`{}` must be a floating point literal",
            key_name(&meta.path)
        ),
    )
}

/// Turn a `crate = "..."` override into a path usable from generated code.
///
/// Bare crate names gain a leading `::`; `crate::`, `self::` and `super::`
/// prefixed paths are used verbatim.
pub(crate) fn parse_crate_path(lit: &LitStr) -> syn::Result<Path> {
    let mut path: Path = lit.parse().map_err(|_| {
        syn::Error::new(
            lit.span(),
            format!("`{}` is not a valid module path", lit.value()),
        )
    })?;
    let relative = path
        .segments
        .first()
        .is_some_and(|s| ["crate", "self", "super"].contains(&s.ident.to_string().as_str()));
    if path.leading_colon.is_none() && !relative {
        path.leading_colon = Some(<syn::Token![::]>::default());
    }
    Ok(path)
}

/// Package name of the facade crate, which re-exports the core crate as `__core`.
const FACADE_PACKAGE: &str = "queuey";
/// Package name of the core crate itself.
const CORE_PACKAGE: &str = "queuey-core";

/// The default path to the core crate, resolved against the *user's* manifest.
///
/// Resolution order, so that depending on either crate alone just works:
///
/// 1. `queuey` (the facade): emits `::queuey::__core`, honouring
///    a `package = ` rename of the dependency.
/// 2. `queuey-core`: emits `::queuey_core`, again honouring a rename.
/// 3. Neither found (no manifest, a vendored tree, ...): falls back to
///    `::queuey_core`.
///
/// An explicit `crate = "..."` in the attribute always wins over all of this.
pub(crate) fn default_crate_path() -> Path {
    resolved_crate_path().unwrap_or_else(|| syn::parse_quote!(::queuey_core))
}

fn resolved_crate_path() -> Option<Path> {
    // The facade first: a crate depending on both should still go through it, and a
    // crate depending only on the facade cannot name the core crate at all.
    if let Ok(found) = proc_macro_crate::crate_name(FACADE_PACKAGE) {
        let facade = match found {
            // `Itself` means we are expanding inside the facade's own lib, examples or
            // doctests; `extern crate self as queuey;` makes this path resolve
            // there too.
            proc_macro_crate::FoundCrate::Itself => "queuey".to_owned(),
            proc_macro_crate::FoundCrate::Name(name) => name,
        };
        return syn::parse_str(&format!("::{facade}::__core")).ok();
    }
    match proc_macro_crate::crate_name(CORE_PACKAGE) {
        Ok(proc_macro_crate::FoundCrate::Itself) => syn::parse_str("crate").ok(),
        Ok(proc_macro_crate::FoundCrate::Name(name)) => syn::parse_str(&format!("::{name}")).ok(),
        Err(_) => None,
    }
}

/// Fully resolved `retry(...)` configuration.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RetrySpec {
    pub(crate) max_attempts: u32,
    pub(crate) backoff: BackoffSpec,
}

/// Fully resolved backoff, with all defaults applied.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BackoffSpec {
    None,
    Fixed {
        delay_ms: u64,
    },
    Exponential {
        base_ms: u64,
        factor: f64,
        max_ms: u64,
        jitter: bool,
    },
}

impl RetrySpec {
    /// Render `RetryPolicy::new(..)` against the resolved core crate path.
    pub(crate) fn to_tokens(&self, core: &Path) -> TokenStream {
        let max_attempts = self.max_attempts;
        let backoff = match &self.backoff {
            BackoffSpec::None => quote!(#core::Backoff::None),
            BackoffSpec::Fixed { delay_ms } => {
                let delay = duration::to_tokens(*delay_ms);
                quote!(#core::Backoff::Fixed(#delay))
            }
            BackoffSpec::Exponential {
                base_ms,
                factor,
                max_ms,
                jitter,
            } => {
                let base = duration::to_tokens(*base_ms);
                let max = duration::to_tokens(*max_ms);
                let factor = LitFloat::new(&format!("{factor:?}f64"), Span::call_site());
                quote! {
                    #core::Backoff::Exponential {
                        base: #base,
                        factor: #factor,
                        max: #max,
                        jitter: #jitter,
                    }
                }
            }
        };
        quote!(#core::RetryPolicy::new(#max_attempts, #backoff))
    }
}

/// Raw, unvalidated `retry(...)` keys.
#[derive(Debug, Default)]
struct RetryRaw {
    max_attempts: Option<Keyed<u32>>,
    backoff: Option<Keyed<String>>,
    delay: Option<Keyed<u64>>,
    base: Option<Keyed<u64>>,
    factor: Option<Keyed<f64>>,
    max: Option<Keyed<u64>>,
    jitter: Option<Keyed<bool>>,
}

/// Parse the body of a `retry(...)` list.
pub(crate) fn parse_retry(meta: &ParseNestedMeta) -> syn::Result<RetrySpec> {
    let list_span = meta.path.span();
    let mut raw = RetryRaw::default();
    // Parsed through `meta::parser` rather than `meta.parse_nested_meta` so that
    // an empty `retry()` is accepted and uses every default.
    let content;
    syn::parenthesized!(content in meta.input);
    let tokens: TokenStream = content.parse()?;
    syn::meta::parser(|inner| {
        if inner.path.is_ident("max_attempts") {
            let value = lit_int::<u32>(&inner, MAX_ATTEMPTS_RANGE)?;
            set_once(&mut raw.max_attempts, &inner, value)
        } else if inner.path.is_ident("backoff") {
            let value = lit_str(&inner)?;
            set_once(&mut raw.backoff, &inner, value.value())
        } else if inner.path.is_ident("delay") {
            let value = duration::parse_lit(&lit_str(&inner)?, "delay")?;
            set_once(&mut raw.delay, &inner, value)
        } else if inner.path.is_ident("base") {
            let value = duration::parse_lit(&lit_str(&inner)?, "base")?;
            set_once(&mut raw.base, &inner, value)
        } else if inner.path.is_ident("factor") {
            let value = lit_f64(&inner)?;
            set_once(&mut raw.factor, &inner, value)
        } else if inner.path.is_ident("max") {
            let value = duration::parse_lit(&lit_str(&inner)?, "max")?;
            set_once(&mut raw.max, &inner, value)
        } else if inner.path.is_ident("jitter") {
            let value = lit_bool(&inner)?;
            set_once(&mut raw.jitter, &inner, value)
        } else {
            Err(inner.error(format!(
                "unknown key `{}` in `retry(...)`, expected one of `max_attempts`, \
                 `backoff`, `delay`, `base`, `factor`, `max`, `jitter`",
                key_name(&inner.path)
            )))
        }
    })
    .parse2(tokens)?;
    raw.finish(list_span)
}

impl RetryRaw {
    fn finish(self, list_span: Span) -> syn::Result<RetrySpec> {
        let max_attempts = match self.max_attempts {
            Some(keyed) if keyed.value == 0 => {
                return Err(syn::Error::new(
                    keyed.span,
                    "`max_attempts` must be >= 1; 1 means no retries",
                ));
            }
            Some(keyed) => keyed.value,
            None => DEFAULT_MAX_ATTEMPTS,
        };

        let (kind, kind_span) = match &self.backoff {
            Some(keyed) => (keyed.value.as_str(), keyed.span),
            None => ("exponential", list_span),
        };

        let backoff = match kind {
            "none" => {
                reject(&self.delay, "delay", "fixed")?;
                reject(&self.base, "base", "exponential")?;
                reject(&self.factor, "factor", "exponential")?;
                reject(&self.max, "max", "exponential")?;
                reject(&self.jitter, "jitter", "exponential")?;
                BackoffSpec::None
            }
            "fixed" => {
                reject(&self.base, "base", "exponential")?;
                reject(&self.factor, "factor", "exponential")?;
                reject(&self.max, "max", "exponential")?;
                reject(&self.jitter, "jitter", "exponential")?;
                let delay = self.delay.ok_or_else(|| {
                    syn::Error::new(
                        kind_span,
                        "`backoff = \"fixed\"` requires `delay = \"...\"`",
                    )
                })?;
                BackoffSpec::Fixed {
                    delay_ms: delay.value,
                }
            }
            "exponential" => {
                reject(&self.delay, "delay", "fixed")?;
                let factor = match self.factor {
                    Some(keyed) => {
                        if keyed.value <= 0.0 || !keyed.value.is_finite() {
                            return Err(syn::Error::new(
                                keyed.span,
                                "`factor` must be a finite number greater than 0",
                            ));
                        }
                        keyed.value
                    }
                    None => DEFAULT_EXP_FACTOR,
                };
                let base_ms = self.base.map_or(DEFAULT_EXP_BASE_MS, |k| k.value);
                let max_ms = self.max.map_or(DEFAULT_EXP_MAX_MS, |k| k.value);
                if base_ms > max_ms {
                    // Point at whichever of the two the user actually wrote.
                    let span = self
                        .base
                        .map(|k| k.span)
                        .or_else(|| self.max.map(|k| k.span))
                        .unwrap_or(kind_span);
                    return Err(syn::Error::new(span, "`base` must be <= `max`"));
                }
                BackoffSpec::Exponential {
                    base_ms,
                    factor,
                    max_ms,
                    jitter: self.jitter.map_or(DEFAULT_EXP_JITTER, |k| k.value),
                }
            }
            other => {
                return Err(syn::Error::new(
                    kind_span,
                    format!(
                        "unknown backoff `{other}`, expected \"none\", \"fixed\" or \"exponential\""
                    ),
                ));
            }
        };

        Ok(RetrySpec {
            max_attempts,
            backoff,
        })
    }
}

fn reject<T>(slot: &Option<Keyed<T>>, key: &str, required_kind: &str) -> syn::Result<()> {
    match slot {
        Some(keyed) => Err(syn::Error::new(
            keyed.span,
            format!("`{key}` is only valid with `backoff = \"{required_kind}\"`"),
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::Attribute;

    /// Parse `#[x(retry(...))]` through the real nested-meta machinery.
    fn retry_of(attr: Attribute) -> syn::Result<RetrySpec> {
        let mut found = None;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("retry") {
                found = Some(parse_retry(&meta)?);
                Ok(())
            } else {
                Err(meta.error("unexpected"))
            }
        })?;
        Ok(found.expect("retry key missing"))
    }

    fn err_of(attr: Attribute) -> String {
        retry_of(attr).unwrap_err().to_string()
    }

    #[test]
    fn defaults_to_exponential() {
        let spec = retry_of(syn::parse_quote!(#[job(retry(max_attempts = 5))])).unwrap();
        assert_eq!(
            spec,
            RetrySpec {
                max_attempts: 5,
                backoff: BackoffSpec::Exponential {
                    base_ms: 1_000,
                    factor: 2.0,
                    max_ms: 300_000,
                    jitter: true,
                },
            }
        );
    }

    #[test]
    fn empty_retry_uses_all_defaults() {
        let spec = retry_of(syn::parse_quote!(#[job(retry())])).unwrap();
        assert_eq!(spec.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert!(matches!(spec.backoff, BackoffSpec::Exponential { .. }));
    }

    #[test]
    fn exponential_keys_are_honoured() {
        let spec = retry_of(syn::parse_quote!(#[job(retry(
            max_attempts = 7,
            backoff = "exponential",
            base = "250ms",
            factor = 1.5,
            max = "2m",
            jitter = false,
        ))]))
        .unwrap();
        assert_eq!(
            spec,
            RetrySpec {
                max_attempts: 7,
                backoff: BackoffSpec::Exponential {
                    base_ms: 250,
                    factor: 1.5,
                    max_ms: 120_000,
                    jitter: false,
                },
            }
        );
    }

    #[test]
    fn integer_factor_is_accepted() {
        let spec = retry_of(syn::parse_quote!(#[job(retry(factor = 3))])).unwrap();
        assert!(matches!(
            spec.backoff,
            BackoffSpec::Exponential { factor, .. } if factor == 3.0
        ));
    }

    #[test]
    fn fixed_backoff() {
        let spec = retry_of(syn::parse_quote!(#[job(retry(backoff = "fixed", delay = "1500ms"))]))
            .unwrap();
        assert_eq!(
            spec,
            RetrySpec {
                max_attempts: 3,
                backoff: BackoffSpec::Fixed { delay_ms: 1_500 },
            }
        );
    }

    #[test]
    fn none_backoff() {
        let spec =
            retry_of(syn::parse_quote!(#[job(retry(max_attempts = 1, backoff = "none"))])).unwrap();
        assert_eq!(spec.backoff, BackoffSpec::None);
    }

    #[test]
    fn rejects_unknown_key() {
        let err = err_of(syn::parse_quote!(#[job(retry(attempts = 3))]));
        assert!(err.contains("unknown key `attempts`"), "{err}");
    }

    #[test]
    fn rejects_duplicate_key() {
        let err = err_of(syn::parse_quote!(#[job(retry(max_attempts = 1, max_attempts = 2))]));
        assert!(err.contains("duplicate key `max_attempts`"), "{err}");
    }

    #[test]
    fn rejects_unknown_backoff() {
        let err = err_of(syn::parse_quote!(#[job(retry(backoff = "linear"))]));
        assert!(err.contains("unknown backoff `linear`"), "{err}");
    }

    #[test]
    fn rejects_delay_on_non_fixed() {
        let err = err_of(syn::parse_quote!(#[job(retry(backoff = "exponential", delay = "1s"))]));
        assert!(err.contains("`delay` is only valid"), "{err}");
        let err = err_of(syn::parse_quote!(#[job(retry(backoff = "none", delay = "1s"))]));
        assert!(err.contains("`delay` is only valid"), "{err}");
    }

    #[test]
    fn rejects_exponential_keys_on_non_exponential() {
        for attr in [
            syn::parse_quote!(#[job(retry(backoff = "fixed", delay = "1s", base = "1s"))]),
            syn::parse_quote!(#[job(retry(backoff = "fixed", delay = "1s", factor = 2.0))]),
            syn::parse_quote!(#[job(retry(backoff = "fixed", delay = "1s", max = "1s"))]),
            syn::parse_quote!(#[job(retry(backoff = "none", jitter = true))]),
        ] {
            let err = err_of(attr);
            assert!(err.contains("is only valid with `backoff ="), "{err}");
        }
    }

    #[test]
    fn fixed_requires_delay() {
        let err = err_of(syn::parse_quote!(#[job(retry(backoff = "fixed"))]));
        assert!(err.contains("requires `delay"), "{err}");
    }

    #[test]
    fn rejects_non_positive_factor() {
        let err = err_of(syn::parse_quote!(#[job(retry(factor = 0.0))]));
        assert!(err.contains("greater than 0"), "{err}");
        let err = err_of(syn::parse_quote!(#[job(retry(factor = -2.0))]));
        assert!(err.contains("greater than 0"), "{err}");
    }

    #[test]
    fn rejects_wrong_literal_types() {
        let err = err_of(syn::parse_quote!(#[job(retry(backoff = 3))]));
        assert!(err.contains("must be a string literal"), "{err}");
        let err = err_of(syn::parse_quote!(#[job(retry(jitter = "yes"))]));
        assert!(err.contains("must be `true` or `false`"), "{err}");
        let err = err_of(syn::parse_quote!(#[job(retry(max_attempts = "3"))]));
        assert!(
            err.contains("`max_attempts` must be an integer between 1 and 4294967295"),
            "{err}"
        );
    }

    #[test]
    fn rejects_out_of_range_max_attempts() {
        for attr in [
            syn::parse_quote!(#[job(retry(max_attempts = 99999999999))]),
            syn::parse_quote!(#[job(retry(max_attempts = -1))]),
            syn::parse_quote!(#[job(retry(max_attempts = 1.5))]),
        ] {
            let err = err_of(attr);
            assert!(
                err.contains("`max_attempts` must be an integer between 1 and 4294967295"),
                "{err}"
            );
        }
    }

    #[test]
    fn rejects_zero_max_attempts() {
        let err = err_of(syn::parse_quote!(#[job(retry(max_attempts = 0))]));
        assert!(
            err.contains("`max_attempts` must be >= 1; 1 means no retries"),
            "{err}"
        );
        // Still rejected when it is the only sensible-looking key around.
        let err = err_of(syn::parse_quote!(#[job(retry(max_attempts = 0, backoff = "none"))]));
        assert!(err.contains("must be >= 1"), "{err}");
    }

    #[test]
    fn rejects_base_greater_than_max() {
        let err = err_of(syn::parse_quote!(#[job(retry(base = "10s", max = "1s"))]));
        assert!(err.contains("`base` must be <= `max`"), "{err}");
        // `base` alone can already exceed the default `max` of 5m.
        let err = err_of(syn::parse_quote!(#[job(retry(base = "10m"))]));
        assert!(err.contains("`base` must be <= `max`"), "{err}");
        // Equal is fine.
        assert!(retry_of(syn::parse_quote!(#[job(retry(base = "1s", max = "1s"))])).is_ok());
    }

    #[test]
    fn rejects_zero_durations() {
        for attr in [
            syn::parse_quote!(#[job(retry(backoff = "fixed", delay = "0"))]),
            syn::parse_quote!(#[job(retry(base = "0s"))]),
            syn::parse_quote!(#[job(retry(max = "0ms"))]),
        ] {
            let err = err_of(attr);
            assert!(err.contains("must be greater than zero"), "{err}");
        }
    }

    #[test]
    fn rejects_bad_duration() {
        let err = err_of(syn::parse_quote!(#[job(retry(base = "soon"))]));
        assert!(err.contains("invalid duration `soon`"), "{err}");
    }

    #[test]
    fn renders_policy_tokens() {
        let core = default_crate_path();
        let spec = RetrySpec {
            max_attempts: 4,
            backoff: BackoffSpec::Exponential {
                base_ms: 1_000,
                factor: 2.0,
                max_ms: 300_000,
                jitter: true,
            },
        };
        let rendered = spec.to_tokens(&core).to_string();
        assert!(rendered.contains("RetryPolicy :: new (4u32"), "{rendered}");
        assert!(rendered.contains("factor : 2.0f64"), "{rendered}");
        assert!(rendered.contains("from_millis (300000u64)"), "{rendered}");

        let fixed = RetrySpec {
            max_attempts: 2,
            backoff: BackoffSpec::Fixed { delay_ms: 500 },
        };
        assert!(
            fixed
                .to_tokens(&core)
                .to_string()
                .contains("Backoff :: Fixed (:: core :: time :: Duration :: from_millis (500u64))")
        );

        let none = RetrySpec {
            max_attempts: 1,
            backoff: BackoffSpec::None,
        };
        assert!(
            none.to_tokens(&core)
                .to_string()
                .contains("Backoff :: None")
        );
    }

    #[test]
    fn crate_path_variants() {
        let bare: Path = parse_crate_path(&syn::parse_quote!("queuey")).unwrap();
        assert_eq!(quote!(#bare).to_string(), ":: queuey");

        let nested: Path = parse_crate_path(&syn::parse_quote!("my_crate::deep::core")).unwrap();
        assert_eq!(quote!(#nested).to_string(), ":: my_crate :: deep :: core");

        let relative: Path = parse_crate_path(&syn::parse_quote!("crate::vendored")).unwrap();
        assert_eq!(quote!(#relative).to_string(), "crate :: vendored");

        let absolute: Path = parse_crate_path(&syn::parse_quote!("::already_absolute")).unwrap();
        assert_eq!(quote!(#absolute).to_string(), ":: already_absolute");

        assert!(parse_crate_path(&syn::parse_quote!("not a path")).is_err());
    }

    #[test]
    fn default_core_path() {
        let path = default_crate_path();
        assert_eq!(quote!(#path).to_string(), ":: queuey_core");
    }
}
