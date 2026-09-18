//! Expansion of `#[derive(Job)]`.

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::spanned::Spanned;
use syn::{Attribute, DeriveInput, Meta, Path};

use crate::attrs::{
    Keyed, RetrySpec, default_crate_path, key_name, lit_str, parse_crate_path, parse_retry,
    reject_blank, respan, set_once,
};

/// Parsed `#[job(...)]` options.
#[derive(Debug, Default)]
pub(crate) struct JobAttr {
    pub(crate) queue: Option<Keyed<Path>>,
    pub(crate) name: Option<Keyed<String>>,
    pub(crate) retry: Option<Keyed<RetrySpec>>,
    pub(crate) core: Option<Keyed<Path>>,
}

impl JobAttr {
    /// The resolved path to `queuey-core`.
    ///
    /// `span` is only used to place the diagnostic when neither crate is a
    /// dependency; an explicit `crate = "..."` never consults the manifest, so
    /// it still works in a build `proc-macro-crate` cannot make sense of.
    pub(crate) fn core_path(&self, span: Span) -> syn::Result<Path> {
        match &self.core {
            Some(keyed) => Ok(keyed.value.clone()),
            None => default_crate_path(span, "job"),
        }
    }
}

/// Parse every `#[job(...)]` attribute on the item.
pub(crate) fn parse_job_attr(attrs: &[Attribute]) -> syn::Result<JobAttr> {
    let mut parsed = JobAttr::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("job")) {
        if matches!(attr.meta, Meta::Path(_)) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("queue") {
                let path: Path = meta.value()?.parse()?;
                let span = path.span();
                set_once(&mut parsed.queue, &meta, path)?;
                if let Some(keyed) = parsed.queue.as_mut() {
                    keyed.span = span;
                }
                Ok(())
            } else if meta.path.is_ident("name") {
                let lit = lit_str(&meta)?;
                // Same rule as `#[queue(name = ...)]`, and for a stronger
                // reason: this string is `Job::NAME`, which lands in every
                // envelope's `job_type` and is the key the worker dispatches
                // handlers on. A blank one is unroutable and unreadable.
                reject_blank(&lit, "name")?;
                set_once(&mut parsed.name, &meta, lit.value())?;
                respan(&mut parsed.name, lit.span());
                Ok(())
            } else if meta.path.is_ident("retry") {
                let spec = parse_retry(&meta)?;
                set_once(&mut parsed.retry, &meta, spec)
            } else if meta.path.is_ident("crate") {
                let lit = lit_str(&meta)?;
                let path = parse_crate_path(&lit)?;
                set_once(&mut parsed.core, &meta, path)
            } else {
                Err(meta.error(format!(
                    "unknown key `{}` in `#[job(...)]`, expected one of `queue`, `name`, \
                     `retry`, `crate`",
                    key_name(&meta.path)
                )))
            }
        })?;
    }
    Ok(parsed)
}

/// Split `AppQueues::Emails` into the queue set type and the variant path.
pub(crate) fn queue_set_of(path: &Path) -> syn::Result<Path> {
    if path.segments.len() < 2 {
        return Err(syn::Error::new(
            path.span(),
            "`queue` must be a path to an enum variant such as `AppQueues::Emails`",
        ));
    }
    let mut set = path.clone();
    set.segments.pop();
    // syn's `pop` leaves the separator behind; drop it so the path prints cleanly.
    set.segments.pop_punct();
    Ok(set)
}

/// Expand `#[derive(Job)]`.
pub(crate) fn derive(input: &DeriveInput) -> syn::Result<TokenStream> {
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            input.generics.span(),
            "generic job types are not supported",
        ));
    }

    let attr = parse_job_attr(&input.attrs)?;
    let core = attr.core_path(input.ident.span())?;

    let queue = attr.queue.as_ref().ok_or_else(|| {
        let span = input
            .attrs
            .iter()
            .find(|a| a.path().is_ident("job"))
            .map_or_else(|| input.ident.span(), Spanned::span);
        syn::Error::new(
            span,
            "#[derive(Job)] requires `#[job(queue = MyQueues::Variant)]`",
        )
    })?;
    let queue_path = &queue.value;
    let queue_set = queue_set_of(queue_path)?;

    let ident = &input.ident;
    let name = match &attr.name {
        Some(keyed) => {
            let literal = syn::LitStr::new(&keyed.value, keyed.span);
            quote!(#literal)
        }
        None => quote!(::core::concat!(
            ::core::module_path!(),
            "::",
            ::core::stringify!(#ident)
        )),
    };

    let retry_policy = attr.retry.as_ref().map(|keyed| {
        let policy = keyed.value.to_tokens(&core);
        quote! {
            fn retry_policy() -> ::core::option::Option<#core::RetryPolicy> {
                ::core::option::Option::Some(#policy)
            }
        }
    });

    Ok(quote! {
        #[automatically_derived]
        // A generated `factor` such as `3.14159265358979f64` is not the author
        // approximating a constant, it is the value they asked for.
        #[allow(clippy::approx_constant)]
        impl #core::Job for #ident {
            type Queue = #queue_set;
            const NAME: &'static ::core::primitive::str = #name;
            const QUEUE: Self::Queue = #queue_path;
            #retry_policy
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attrs::BackoffSpec;

    fn job_of(attrs: TokenStream) -> syn::Result<JobAttr> {
        let input: DeriveInput = syn::parse_quote! {
            #attrs
            struct SendEmail { to: String }
        };
        parse_job_attr(&input.attrs)
    }

    fn expand(input: DeriveInput) -> syn::Result<String> {
        derive(&input).map(|tokens| tokens.to_string())
    }

    #[test]
    fn parses_all_keys() {
        let parsed = job_of(quote!(#[job(
            queue = AppQueues::Emails,
            name = "emails.send",
            retry(max_attempts = 5),
            crate = "queuey",
        )]))
        .unwrap();
        let queue_tokens = &parsed.queue.as_ref().unwrap().value;
        assert_eq!(
            quote!(#queue_tokens).to_string(),
            "AppQueues :: Emails".to_owned()
        );
        assert_eq!(parsed.name.as_ref().unwrap().value, "emails.send");
        assert_eq!(parsed.retry.as_ref().unwrap().value.max_attempts, 5);
        let core = parsed.core_path(Span::call_site()).unwrap();
        assert_eq!(quote!(#core).to_string(), ":: queuey");
    }

    #[test]
    fn defaults_to_core_crate() {
        let parsed = job_of(quote!(#[job(queue = AppQueues::Emails)])).unwrap();
        let core = parsed.core_path(Span::call_site()).unwrap();
        assert_eq!(quote!(#core).to_string(), ":: queuey_core");
        assert!(parsed.retry.is_none());
        assert!(parsed.name.is_none());
    }

    #[test]
    fn rejects_unknown_and_duplicate_keys() {
        let err = job_of(quote!(#[job(queue = A::B, kind = "x")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown key `kind`"), "{err}");

        let err = job_of(quote!(#[job(queue = A::B, queue = A::C)]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("duplicate key `queue`"), "{err}");
    }

    #[test]
    fn rejects_a_blank_name() {
        // `Job::NAME` is the envelope's `job_type` and the handler-dispatch
        // key; a blank one routes to nothing and reads as nothing.
        let err = job_of(quote!(#[job(queue = A::B, name = "")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`name` must not be empty"), "{err}");

        let err = job_of(quote!(#[job(queue = A::B, name = "   ")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`name` must not be only whitespace"), "{err}");

        // A name with inner spaces is unusual but routable, so it stays legal.
        assert!(job_of(quote!(#[job(queue = A::B, name = "send email")])).is_ok());
    }

    #[test]
    fn queue_set_is_the_path_minus_the_variant() {
        let path: Path = syn::parse_quote!(AppQueues::Emails);
        let set = queue_set_of(&path).unwrap();
        assert_eq!(quote!(#set).to_string(), "AppQueues");

        let path: Path = syn::parse_quote!(crate::queues::AppQueues::Emails);
        let set = queue_set_of(&path).unwrap();
        assert_eq!(
            quote!(#set).to_string(),
            "crate :: queues :: AppQueues".to_owned()
        );

        let path: Path = syn::parse_quote!(Emails);
        let err = queue_set_of(&path).unwrap_err().to_string();
        assert!(err.contains("path to an enum variant"), "{err}");
    }

    #[test]
    fn expands_with_defaults() {
        let expanded = expand(syn::parse_quote! {
            #[job(queue = AppQueues::Emails)]
            struct SendEmail { to: String }
        })
        .unwrap();
        assert!(expanded.contains("Job for SendEmail"), "{expanded}");
        assert!(expanded.contains("type Queue = AppQueues"), "{expanded}");
        assert!(
            expanded.contains("const QUEUE : Self :: Queue = AppQueues :: Emails"),
            "{expanded}"
        );
        assert!(expanded.contains("module_path !"), "{expanded}");
        assert!(!expanded.contains("retry_policy"), "{expanded}");
    }

    #[test]
    fn expands_with_overrides() {
        let expanded = expand(syn::parse_quote! {
            #[job(queue = AppQueues::Emails, name = "send_email", retry(backoff = "none"))]
            enum SendEmail { A }
        })
        .unwrap();
        assert!(expanded.contains("\"send_email\""), "{expanded}");
        assert!(expanded.contains("fn retry_policy ()"), "{expanded}");
        assert!(expanded.contains("Backoff :: None"), "{expanded}");
    }

    #[test]
    fn rejects_missing_queue() {
        let err = expand(syn::parse_quote! {
            #[job(name = "x")]
            struct SendEmail;
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("requires `#[job(queue"), "{err}");

        let err = expand(syn::parse_quote! {
            struct SendEmail;
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("requires `#[job(queue"), "{err}");
    }

    #[test]
    fn rejects_generics() {
        let err = expand(syn::parse_quote! {
            #[job(queue = AppQueues::Emails)]
            struct SendEmail<T> { payload: T }
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("generic job types are not supported"), "{err}");

        let err = expand(syn::parse_quote! {
            #[job(queue = AppQueues::Emails)]
            struct SendEmail where String: Clone { to: String }
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("generic job types are not supported"), "{err}");
    }

    #[test]
    fn retry_grammar_is_shared_with_queues() {
        let parsed = job_of(quote!(#[job(
            queue = A::B,
            retry(backoff = "fixed", delay = "250ms", max_attempts = 4)
        )]))
        .unwrap();
        let retry = parsed.retry.unwrap().value;
        assert_eq!(retry.max_attempts, 4);
        assert_eq!(retry.backoff, BackoffSpec::Fixed { delay_ms: 250 });
    }
}
