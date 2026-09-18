//! Expansion of `#[derive(Queues)]`.

use std::collections::HashMap;

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::spanned::Spanned;
use syn::{Attribute, Data, DeriveInput, Fields, Ident, LitStr, Meta, Path, Variant};

use crate::attrs::{
    Keyed, MAX_MESSAGE_TTL_MS, MAX_PRIORITY_RANGE, MESSAGE_TTL_RANGE, PREFETCH_RANGE, RetrySpec,
    default_crate_path, key_name, lit_bool, lit_int, lit_str, parse_crate_path, parse_retry,
    reject_blank, respan, set_once,
};
use crate::duration;

/// Container level `#[queues(...)]` options.
#[derive(Debug, Default)]
pub(crate) struct ContainerAttr {
    pub(crate) prefix: Option<Keyed<String>>,
    pub(crate) core: Option<Keyed<Path>>,
}

impl ContainerAttr {
    /// The resolved path to `queuey-core`.
    ///
    /// `span` is only used to place the diagnostic when neither crate is a
    /// dependency; an explicit `crate = "..."` never consults the manifest, so
    /// it still works in a build `proc-macro-crate` cannot make sense of.
    pub(crate) fn core_path(&self, span: Span) -> syn::Result<Path> {
        match &self.core {
            Some(keyed) => Ok(keyed.value.clone()),
            None => default_crate_path(span, "queues"),
        }
    }

    /// Apply the optional prefix to a bare queue name.
    ///
    /// The `.` between the two is added here and nowhere else, which is why a
    /// `prefix` ending in one is rejected at parse time: writing `prefix = "a."`
    /// used to produce `a..b`, a legal but nonsensical broker queue name that
    /// nothing would ever have noticed until an operator looked at the
    /// management UI.
    pub(crate) fn qualify(&self, name: &str) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}.{}", prefix.value, name),
            None => name.to_owned(),
        }
    }
}

/// Separator [`ContainerAttr::qualify`] puts between the prefix and the name.
const PREFIX_SEPARATOR: char = '.';

/// Variant level `#[queue(...)]` options.
#[derive(Debug, Default)]
pub(crate) struct QueueAttr {
    pub(crate) name: Option<Keyed<String>>,
    pub(crate) prefetch: Option<Keyed<u16>>,
    pub(crate) durable: Option<Keyed<bool>>,
    pub(crate) message_ttl: Option<Keyed<u64>>,
    pub(crate) max_priority: Option<Keyed<u8>>,
    pub(crate) retry: Option<Keyed<RetrySpec>>,
}

/// Parse every `#[queues(...)]` attribute on the enum.
pub(crate) fn parse_container_attr(attrs: &[Attribute]) -> syn::Result<ContainerAttr> {
    let mut parsed = ContainerAttr::default();
    reject_misplaced(attrs, "queue", "queues", "a variant", "the enum")?;
    for attr in attrs.iter().filter(|a| a.path().is_ident("queues")) {
        if matches!(attr.meta, Meta::Path(_)) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("prefix") {
                let lit = lit_str(&meta)?;
                reject_blank(&lit, "prefix")?;
                reject_trailing_separator(&lit)?;
                set_once(&mut parsed.prefix, &meta, lit.value())?;
                respan(&mut parsed.prefix, lit.span());
                Ok(())
            } else if meta.path.is_ident("crate") {
                let lit = lit_str(&meta)?;
                let path = parse_crate_path(&lit)?;
                set_once(&mut parsed.core, &meta, path)?;
                respan(&mut parsed.core, lit.span());
                Ok(())
            } else {
                Err(meta.error(format!(
                    "unknown key `{}` in `#[queues(...)]`, expected `prefix` or `crate`",
                    key_name(&meta.path)
                )))
            }
        })?;
    }
    Ok(parsed)
}

/// Parse every `#[queue(...)]` attribute on one variant.
pub(crate) fn parse_queue_attr(attrs: &[Attribute]) -> syn::Result<QueueAttr> {
    let mut parsed = QueueAttr::default();
    reject_misplaced(attrs, "queues", "queue", "the enum", "a variant")?;
    for attr in attrs.iter().filter(|a| a.path().is_ident("queue")) {
        if matches!(attr.meta, Meta::Path(_)) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let lit = lit_str(&meta)?;
                reject_blank(&lit, "name")?;
                set_once(&mut parsed.name, &meta, lit.value())?;
                respan(&mut parsed.name, lit.span());
                Ok(())
            } else if meta.path.is_ident("prefetch") {
                let value = lit_int::<u16>(&meta, PREFETCH_RANGE)?;
                if value == 0 {
                    return Err(meta.error(
                        "`prefetch = 0` means unlimited in AMQP; omit the attribute or use a \
                         positive value",
                    ));
                }
                set_once(&mut parsed.prefetch, &meta, value)
            } else if meta.path.is_ident("durable") {
                let value = lit_bool(&meta)?;
                set_once(&mut parsed.durable, &meta, value)
            } else if meta.path.is_ident("message_ttl") {
                let lit = lit_str(&meta)?;
                let millis = duration::parse_lit(&lit, "message_ttl")?;
                if millis > MAX_MESSAGE_TTL_MS {
                    return Err(syn::Error::new(
                        lit.span(),
                        format!(
                            "`message_ttl` must be {MESSAGE_TTL_RANGE}: a broker stores it as an \
                             unsigned 32-bit millisecond count, so a longer TTL cannot be \
                             expressed and would be silently clamped"
                        ),
                    ));
                }
                set_once(&mut parsed.message_ttl, &meta, millis)?;
                respan(&mut parsed.message_ttl, lit.span());
                Ok(())
            } else if meta.path.is_ident("max_priority") {
                let value = lit_int::<u8>(&meta, MAX_PRIORITY_RANGE)?;
                set_once(&mut parsed.max_priority, &meta, value)
            } else if meta.path.is_ident("retry") {
                let spec = parse_retry(&meta)?;
                set_once(&mut parsed.retry, &meta, spec)
            } else {
                Err(meta.error(format!(
                    "unknown key `{}` in `#[queue(...)]`, expected one of `name`, `prefetch`, \
                     `durable`, `message_ttl`, `max_priority`, `retry`",
                    key_name(&meta.path)
                )))
            }
        })?;
    }
    Ok(parsed)
}

/// Reject a `prefix` that already ends in the separator `qualify` adds.
///
/// `#[queues(prefix = "a.")]` used to expand to the queue name `a..b`. Rejecting
/// is the better of the two possible fixes: swallowing the extra separator would
/// mean two different sources produce the same broker queue name, and a queue
/// name is wire format an operator reads. Saying so at the offending literal
/// costs the author one character.
fn reject_trailing_separator(lit: &LitStr) -> syn::Result<()> {
    if lit.value().ends_with(PREFIX_SEPARATOR) {
        return Err(syn::Error::new(
            lit.span(),
            format!(
                "`prefix` must not end with `{PREFIX_SEPARATOR}`: the separator before the queue \
                 name is added for you, so `prefix = \"myapp\"` already yields `myapp.emails`"
            ),
        ));
    }
    Ok(())
}

/// Reject a helper attribute that belongs on the other level of the item.
///
/// `#[derive(Queues)]` registers both `queues` and `queue`, which makes both
/// *inert* anywhere on the enum. A `#[queue(...)]` on the container or a
/// `#[queues(...)]` on a variant therefore used to compile clean and be dropped
/// on the floor, unknown keys and all: the author's `prefix` or `prefetch`
/// silently did nothing. `wrong` is the attribute that was written, `right` the
/// one that belongs here.
fn reject_misplaced(
    attrs: &[Attribute],
    wrong: &str,
    right: &str,
    wrong_place: &str,
    right_place: &str,
) -> syn::Result<()> {
    match attrs.iter().find(|a| a.path().is_ident(wrong)) {
        Some(attr) => Err(syn::Error::new_spanned(
            attr.path(),
            format!(
                "`#[{wrong}(...)]` belongs on {wrong_place}; {right_place} takes \
                 `#[{right}(...)]`"
            ),
        )),
        None => Ok(()),
    }
}

/// Convert a variant identifier to its default queue name.
///
/// `SendEmails` becomes `send_emails`, `HTTPCalls` becomes `http_calls`.
pub(crate) fn snake_case(ident: &str) -> String {
    let chars: Vec<char> = ident.chars().collect();
    let mut out = String::with_capacity(chars.len() + 4);
    for (index, &current) in chars.iter().enumerate() {
        if current == '_' {
            if !out.ends_with('_') && !out.is_empty() {
                out.push('_');
            }
            continue;
        }
        if current.is_uppercase() {
            let previous = index.checked_sub(1).map(|i| chars[i]);
            let next = chars.get(index + 1).copied();
            let boundary = match previous {
                None => false,
                Some('_') => false,
                Some(previous) if previous.is_lowercase() || previous.is_ascii_digit() => true,
                // `HTTPCalls`: split before the last capital of a run.
                Some(_) => next.is_some_and(char::is_lowercase),
            };
            if boundary && !out.ends_with('_') {
                out.push('_');
            }
        }
        out.extend(current.to_lowercase());
    }
    out
}

/// One fully resolved queue variant.
struct ResolvedQueue<'a> {
    ident: &'a Ident,
    name: String,
    name_span: Span,
    attr: QueueAttr,
}

/// Expand `#[derive(Queues)]`.
pub(crate) fn derive(input: &DeriveInput) -> syn::Result<TokenStream> {
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new(
            input.ident.span(),
            "#[derive(Queues)] can only be applied to enums",
        ));
    };

    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            input.generics.span(),
            "generic queue enums are not supported",
        ));
    }

    if data.variants.is_empty() {
        return Err(syn::Error::new(
            input.ident.span(),
            "#[derive(Queues)] requires at least one variant",
        ));
    }

    let container = parse_container_attr(&input.attrs)?;
    let core = container.core_path(input.ident.span())?;

    let mut resolved: Vec<ResolvedQueue<'_>> = Vec::with_capacity(data.variants.len());
    for variant in &data.variants {
        resolved.push(resolve_variant(variant, &container)?);
    }

    let mut seen: HashMap<&str, Span> = HashMap::new();
    for queue in &resolved {
        if let Some(previous) = seen.insert(&queue.name, queue.name_span) {
            let mut error = syn::Error::new(
                queue.name_span,
                format!("duplicate queue name `{}`", queue.name),
            );
            error.combine(syn::Error::new(
                previous,
                format!("`{}` is first used here", queue.name),
            ));
            return Err(error);
        }
    }

    let ident = &input.ident;
    let variant_paths = resolved.iter().map(|q| {
        let variant = q.ident;
        quote!(Self::#variant)
    });
    let name_arms = resolved.iter().map(|q| {
        let variant = q.ident;
        let name = &q.name;
        quote!(Self::#variant => #name)
    });
    let config_arms = resolved.iter().map(|q| {
        let variant = q.ident;
        let config = config_expr(q, &core);
        quote!(Self::#variant => #config)
    });

    Ok(quote! {
        #[automatically_derived]
        // A generated `factor` such as `3.14159265358979f64` is not the author
        // approximating a constant, it is the value they asked for.
        #[allow(clippy::approx_constant)]
        impl #core::QueueSet for #ident {
            fn all() -> &'static [Self] {
                &[#(#variant_paths),*]
            }

            fn name(&self) -> &'static ::core::primitive::str {
                match self {
                    #(#name_arms),*
                }
            }

            fn config(&self) -> #core::QueueConfig {
                match self {
                    #(#config_arms),*
                }
            }
        }
    })
}

fn resolve_variant<'a>(
    variant: &'a Variant,
    container: &ContainerAttr,
) -> syn::Result<ResolvedQueue<'a>> {
    if !matches!(variant.fields, Fields::Unit) {
        return Err(syn::Error::new(
            variant.fields.span(),
            "queue variants must not have fields",
        ));
    }

    let attr = parse_queue_attr(&variant.attrs)?;
    let (bare, name_span) = match &attr.name {
        Some(keyed) => (keyed.value.clone(), keyed.span),
        None => (snake_case(&variant.ident.to_string()), variant.ident.span()),
    };

    // `prefix` and `name` are already known to be non-empty, so the only way to
    // get here empty is a variant whose snake_case form has nothing left, such
    // as `__`.
    if bare.is_empty() {
        return Err(syn::Error::new(
            name_span,
            "queue name is empty; give the variant a `#[queue(name = \"...\")]`",
        ));
    }

    Ok(ResolvedQueue {
        ident: &variant.ident,
        name: container.qualify(&bare),
        name_span,
        attr,
    })
}

fn config_expr(queue: &ResolvedQueue<'_>, core: &Path) -> TokenStream {
    let name = &queue.name;
    let mut expr = quote!(#core::QueueConfig::new(#name));
    if let Some(prefetch) = &queue.attr.prefetch {
        let value = prefetch.value;
        expr = quote!(#expr.prefetch(#value));
    }
    if let Some(durable) = &queue.attr.durable {
        let value = durable.value;
        expr = quote!(#expr.durable(#value));
    }
    if let Some(ttl) = &queue.attr.message_ttl {
        let value = duration::to_tokens(ttl.value);
        expr = quote!(#expr.message_ttl(#value));
    }
    if let Some(max_priority) = &queue.attr.max_priority {
        let value = max_priority.value;
        expr = quote!(#expr.max_priority(#value));
    }
    if let Some(retry) = &queue.attr.retry {
        let policy = retry.value.to_tokens(core);
        expr = quote!(#expr.retry(#policy));
    }
    expr
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attrs::BackoffSpec;

    fn container_of(attrs: TokenStream) -> syn::Result<ContainerAttr> {
        let input: DeriveInput = syn::parse_quote! {
            #attrs
            enum Q { A }
        };
        parse_container_attr(&input.attrs)
    }

    fn queue_of(attrs: TokenStream) -> syn::Result<QueueAttr> {
        let input: DeriveInput = syn::parse_quote! {
            enum Q {
                #attrs
                A,
            }
        };
        let Data::Enum(data) = &input.data else {
            unreachable!()
        };
        parse_queue_attr(&data.variants[0].attrs)
    }

    #[test]
    fn snake_case_conversions() {
        assert_eq!(snake_case("Emails"), "emails");
        assert_eq!(snake_case("SendEmails"), "send_emails");
        assert_eq!(snake_case("HTTPCalls"), "http_calls");
        assert_eq!(snake_case("HTTP"), "http");
        assert_eq!(snake_case("A"), "a");
        assert_eq!(snake_case("ImageResize2"), "image_resize2");
        assert_eq!(snake_case("Resize2xImages"), "resize2x_images");
        assert_eq!(snake_case("Already_Snake"), "already_snake");
        assert_eq!(snake_case("already_snake"), "already_snake");
        assert_eq!(snake_case("XMLHTTPRequest"), "xmlhttp_request");
    }

    #[test]
    fn container_defaults_to_core_crate() {
        let parsed = container_of(quote!()).unwrap();
        assert!(parsed.prefix.is_none());
        let core = parsed.core_path(Span::call_site()).unwrap();
        assert_eq!(quote!(#core).to_string(), ":: queuey_core");
        assert_eq!(parsed.qualify("emails"), "emails");
    }

    #[test]
    fn container_prefix_and_crate() {
        let parsed = container_of(quote!(#[queues(prefix = "myapp", crate = "queuey")])).unwrap();
        assert_eq!(parsed.qualify("emails"), "myapp.emails");
        let core = parsed.core_path(Span::call_site()).unwrap();
        assert_eq!(quote!(#core).to_string(), ":: queuey");
    }

    #[test]
    fn container_rejects_unknown_and_duplicate_keys() {
        let err = container_of(quote!(#[queues(suffix = "x")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown key `suffix`"), "{err}");

        let err = container_of(quote!(#[queues(prefix = "a", prefix = "b")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("duplicate key `prefix`"), "{err}");
    }

    #[test]
    fn bare_attribute_is_empty() {
        let parsed = container_of(quote!(#[queues])).unwrap();
        assert!(parsed.prefix.is_none());
        let parsed = queue_of(quote!(#[queue])).unwrap();
        assert!(parsed.name.is_none());
    }

    #[test]
    fn queue_keys_are_parsed() {
        let parsed = queue_of(quote!(#[queue(
            name = "img",
            prefetch = 10,
            durable = false,
            message_ttl = "30s",
            retry(max_attempts = 2, backoff = "fixed", delay = "1s"),
        )]))
        .unwrap();
        assert_eq!(parsed.name.unwrap().value, "img");
        assert_eq!(parsed.prefetch.unwrap().value, 10);
        assert!(!parsed.durable.unwrap().value);
        assert_eq!(parsed.message_ttl.unwrap().value, 30_000);
        let retry = parsed.retry.unwrap().value;
        assert_eq!(retry.max_attempts, 2);
        assert_eq!(retry.backoff, BackoffSpec::Fixed { delay_ms: 1_000 });
    }

    #[test]
    fn queue_rejects_bad_values() {
        for attr in [
            quote!(#[queue(prefetch = 70000)]),
            quote!(#[queue(prefetch = -1)]),
            quote!(#[queue(prefetch = 1.5)]),
            quote!(#[queue(prefetch = "10")]),
        ] {
            let err = queue_of(attr).unwrap_err().to_string();
            assert!(
                err.contains("`prefetch` must be an integer between 1 and 65535"),
                "{err}"
            );
        }

        let err = queue_of(quote!(#[queue(message_ttl = "soon")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid duration"), "{err}");

        let err = queue_of(quote!(#[queue(nope = 1)]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown key `nope`"), "{err}");

        let err = queue_of(quote!(#[queue(name = "a", name = "b")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("duplicate key `name`"), "{err}");
    }

    #[test]
    fn queue_rejects_zero_prefetch() {
        let err = queue_of(quote!(#[queue(prefetch = 0)]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`prefetch = 0` means unlimited in AMQP"),
            "{err}"
        );
        assert!(err.contains("omit the attribute"), "{err}");
    }

    #[test]
    fn queue_accepts_every_max_priority_level() {
        for (attr, expected) in [
            (quote!(#[queue(max_priority = 0)]), 0u8),
            (quote!(#[queue(max_priority = 1)]), 1),
            (quote!(#[queue(max_priority = 255)]), 255),
        ] {
            let parsed = queue_of(attr).unwrap();
            assert_eq!(parsed.max_priority.unwrap().value, expected);
        }
    }

    #[test]
    fn queue_rejects_bad_max_priority() {
        for attr in [
            quote!(#[queue(max_priority = 256)]),
            quote!(#[queue(max_priority = -1)]),
            quote!(#[queue(max_priority = "10")]),
            quote!(#[queue(max_priority = 1.5)]),
            quote!(#[queue(max_priority = true)]),
        ] {
            let err = queue_of(attr).unwrap_err().to_string();
            assert!(
                err.contains("`max_priority` must be an integer in 0..=255"),
                "{err}"
            );
            // Never `syn`'s raw parse failure.
            assert!(!err.contains("invalid digit"), "{err}");
        }

        let err = queue_of(quote!(#[queue(max_priority = 1, max_priority = 2)]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("duplicate key `max_priority`"), "{err}");
    }

    #[test]
    fn max_priority_is_emitted_only_when_written() {
        let input: DeriveInput = syn::parse_quote! {
            enum Q {
                #[queue(max_priority = 3)]
                Priced,
                #[queue(max_priority = 0)]
                Plain,
                Defaulted,
            }
        };
        let expanded = derive(&input).unwrap().to_string();
        assert!(expanded.contains(". max_priority (3u8)"), "{expanded}");
        assert!(expanded.contains(". max_priority (0u8)"), "{expanded}");
        assert_eq!(expanded.matches("max_priority (").count(), 2, "{expanded}");
    }

    #[test]
    fn queue_rejects_zero_message_ttl() {
        for attr in [
            quote!(#[queue(message_ttl = "0")]),
            quote!(#[queue(message_ttl = "0s")]),
            quote!(#[queue(message_ttl = "0ms")]),
        ] {
            let err = queue_of(attr).unwrap_err().to_string();
            assert!(
                err.contains("`message_ttl` must be greater than zero"),
                "{err}"
            );
        }
    }

    #[test]
    fn a_helper_attribute_on_the_wrong_level_is_an_error_not_a_silent_drop() {
        // `#[derive(Queues)]` registers both names, so both are inert anywhere
        // on the item and used to be dropped without a word.
        let err = container_of(quote!(#[queue(prefix = "myapp", nonsense = 1)]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`#[queue(...)]` belongs on a variant"),
            "{err}"
        );
        assert!(err.contains("`#[queues(...)]`"), "{err}");

        let err = queue_of(quote!(#[queues(prefix = "myapp")]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`#[queues(...)]` belongs on the enum"),
            "{err}"
        );
        assert!(err.contains("`#[queue(...)]`"), "{err}");

        // The right attribute in the right place is of course still fine.
        assert!(container_of(quote!(#[queues(prefix = "myapp")])).is_ok());
        assert!(queue_of(quote!(#[queue(prefetch = 4)])).is_ok());
    }

    #[test]
    fn a_prefix_must_not_end_in_the_separator_qualify_adds() {
        let err = container_of(quote!(#[queues(prefix = "a.")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`prefix` must not end with `.`"), "{err}");
        assert!(err.contains("added for you"), "{err}");

        // Dots *inside* a prefix are a normal AMQP namespace and stay legal.
        let parsed = container_of(quote!(#[queues(prefix = "myapp.jobs")])).unwrap();
        assert_eq!(parsed.qualify("emails"), "myapp.jobs.emails");
    }

    #[test]
    fn queue_rejects_a_message_ttl_a_broker_cannot_express() {
        for attr in [
            quote!(#[queue(message_ttl = "100d")]),
            quote!(#[queue(message_ttl = "4294967296ms")]),
        ] {
            let err = queue_of(attr).unwrap_err().to_string();
            assert!(err.contains("`message_ttl` must be between"), "{err}");
            assert!(err.contains("4294967295ms"), "{err}");
            assert!(err.contains("49.7 days"), "{err}");
        }

        // The ceiling itself is accepted.
        let parsed = queue_of(quote!(#[queue(message_ttl = "4294967295ms")])).unwrap();
        assert_eq!(parsed.message_ttl.unwrap().value, MAX_MESSAGE_TTL_MS);
        // And a realistic TTL is nowhere near it.
        assert!(queue_of(quote!(#[queue(message_ttl = "7d")])).is_ok());
    }

    #[test]
    fn empty_names_are_rejected() {
        let err = container_of(quote!(#[queues(prefix = "")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`prefix` must not be empty"), "{err}");

        let err = queue_of(quote!(#[queue(name = "")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`name` must not be empty"), "{err}");

        let all_underscores: DeriveInput = syn::parse_quote! {
            enum Q { __ }
        };
        let err = derive(&all_underscores).unwrap_err().to_string();
        assert!(err.contains("queue name is empty"), "{err}");
    }

    #[test]
    fn whitespace_only_names_are_rejected_too() {
        // `"   "` is a legal AMQP queue name and a completely useless one.
        for text in ["   ", "\t", " \n "] {
            let lit = proc_macro2::Literal::string(text);
            let err = queue_of(quote!(#[queue(name = #lit)]))
                .unwrap_err()
                .to_string();
            assert!(err.contains("`name` must not be only whitespace"), "{err}");

            let err = container_of(quote!(#[queues(prefix = #lit)]))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("`prefix` must not be only whitespace"),
                "{err}"
            );
        }
    }

    #[test]
    fn expansion_contains_expected_pieces() {
        let input: DeriveInput = syn::parse_quote! {
            #[queues(prefix = "myapp")]
            enum AppQueues {
                #[queue(prefetch = 10)]
                Emails,
                #[queue(name = "img", retry(max_attempts = 3))]
                HTTPCalls,
            }
        };
        let expanded = derive(&input).unwrap().to_string();
        assert!(expanded.contains("QueueSet for AppQueues"), "{expanded}");
        assert!(expanded.contains("\"myapp.emails\""), "{expanded}");
        assert!(expanded.contains("\"myapp.img\""), "{expanded}");
        assert!(expanded.contains(". prefetch (10u16)"), "{expanded}");
        assert!(expanded.contains("RetryPolicy :: new (3u32"), "{expanded}");
        assert!(
            expanded.contains("& [Self :: Emails , Self :: HTTPCalls]"),
            "{expanded}"
        );
    }

    #[test]
    fn expansion_rejects_invalid_shapes() {
        let not_enum: DeriveInput = syn::parse_quote!(
            struct S;
        );
        assert!(
            derive(&not_enum)
                .unwrap_err()
                .to_string()
                .contains("can only be applied to enums")
        );

        let with_fields: DeriveInput = syn::parse_quote! {
            enum Q { A(u8) }
        };
        assert!(
            derive(&with_fields)
                .unwrap_err()
                .to_string()
                .contains("must not have fields")
        );

        let empty: DeriveInput = syn::parse_quote! {
            enum Q {}
        };
        assert!(
            derive(&empty)
                .unwrap_err()
                .to_string()
                .contains("at least one variant")
        );

        let generic: DeriveInput = syn::parse_quote! {
            enum Q<T> { A(T) }
        };
        assert!(
            derive(&generic)
                .unwrap_err()
                .to_string()
                .contains("generic queue enums")
        );

        let duplicates: DeriveInput = syn::parse_quote! {
            enum Q {
                #[queue(name = "same")]
                A,
                #[queue(name = "same")]
                B,
            }
        };
        assert!(
            derive(&duplicates)
                .unwrap_err()
                .to_string()
                .contains("duplicate queue name `same`")
        );
    }

    #[test]
    fn default_config_has_no_builder_calls() {
        let input: DeriveInput = syn::parse_quote! {
            enum Q { Emails }
        };
        let expanded = derive(&input).unwrap().to_string();
        assert!(
            expanded.contains("QueueConfig :: new (\"emails\")"),
            "{expanded}"
        );
        assert!(!expanded.contains("prefetch ("), "{expanded}");
        assert!(!expanded.contains("retry ("), "{expanded}");
    }
}
