use env_logger;
use handlebars::{Handlebars, RenderError};
use lazy_static::lazy_static;
use log::warn;
use rusoto_core::{
    credential::{AwsCredentials, DefaultCredentialsProvider, ProvideAwsCredentials},
    Region, RusotoError,
};
use rusoto_s3::{
    util::{PreSignedRequest, PreSignedRequestOption},
    GetObjectRequest, HeadObjectError, ListObjectsV2Error, ListObjectsV2Request,
    S3Client, S3,
};
use serde::Serialize;
use std::{str::FromStr, sync::Arc, time::Duration};
use thiserror::Error;
use warp::{
    http::uri::InvalidUri,
    hyper::Uri,
    path::FullPath,
    reject::Reject,
    Filter,
};
mod utils;
use utils::{get_parent, url_encode};

lazy_static! {
    static ref REGION: Region = Region::Custom {
        name: "local".to_string(),
        endpoint: "http://127.0.0.1:9000".to_string()
    };
}
const BUCKET: &'static str = "testbucket";
const DIR_LIST_TEMPLATE: &'static str = include_str!("directory_listing.hbs");

#[derive(Error, Debug)]
enum DirectoryListingError {
    #[error("template error")]
    TemplateError(#[from] RenderError),
    #[error("s3 error")]
    S3Error(#[from] RusotoError<ListObjectsV2Error>),
}
impl Reject for DirectoryListingError {}

#[derive(Error, Debug)]
enum RequestError {
    #[error("url presigning error")]
    BadPresignedUrl(#[from] InvalidUri),
    #[error("s3 error")]
    S3Error(#[from] RusotoError<HeadObjectError>),
    #[error("encoding error")]
    EncodingError(#[from] std::str::Utf8Error),
}
impl Reject for RequestError {}

struct Ctx {
    s3: Arc<S3Client>,
    credentials: Arc<AwsCredentials>,
    handlebars: Arc<Handlebars<'static>>,
}

#[derive(Serialize)]
struct DirectoryListingItem<'a> {
    name: &'a str,
    url: String,
}

#[derive(Serialize)]
struct DirectoryListingData<'a> {
    title: &'a str,
    path: &'a str,
    parent: &'a str,
    items: Vec<DirectoryListingItem<'a>>,
}

async fn directory_listing(base: &str, ctx: &Ctx) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    debug_assert!(base.is_empty() || base.ends_with('/'));
    // TODO use pagination
    let list = ctx
        .s3
        .list_objects_v2(ListObjectsV2Request {
            bucket: BUCKET.into(),
            prefix: Some(base.into()),
            delimiter: Some("/".into()),
            ..Default::default()
        })
        .await
        .map_err(DirectoryListingError::S3Error)?;
    if list.is_truncated == Some(true) {
        warn!("list of ({}) has too many results", base);
    }
    let get_url = |name: &str| url_encode(&format!("/{}{}", base, name));
    let mut items = vec![];
    if let Some(ref common) = list.common_prefixes {
        items.extend(common.iter().filter_map(|c| {
            // TODO log None here
            let name = c.prefix.as_deref()?.strip_prefix(base)?;
            let url = get_url(name);
            Some(DirectoryListingItem { name, url })
        }));
    }
    if let Some(ref contents) = list.contents {
        items.extend(
            contents
                .iter()
                // TODO handle keys that end with /
                // TODO log None here
                .filter_map(|c| {
                    let name = c.key.as_deref()?.strip_prefix(base)?;
                    let url = get_url(name);
                    Some(DirectoryListingItem { name, url })
                }),
        );
    }
    if items.is_empty() {
        Err(warp::reject::not_found())
    } else {
        let basepath = &format!("/{}", base);
        let parentpath = get_parent(&base);
        let parent = url_encode(&if let Some(parent) = parentpath {
            format!("/{}", parent)
        } else {
            "".into()
        });
        let data = DirectoryListingData {
            title: basepath,
            path: basepath,
            parent: &parent,
            items,
        };
        Ok(Box::new(warp::reply::html(
            ctx.handlebars
                .render("directory_listing", &data)
                .map_err(DirectoryListingError::TemplateError)?,
        )))
    }
}

// TODO is there some way to avoid the box in the return?
async fn request(path: FullPath, ctx: Ctx) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let pathstr = &percent_encoding::percent_decode_str(path.as_str())
        .decode_utf8()
        .map_err(RequestError::EncodingError)?;
    let pathstr = pathstr.strip_prefix("/").ok_or_else(|| warp::reject::not_found())?;
    if pathstr.is_empty() || pathstr.ends_with("/") {
        directory_listing(pathstr, &ctx).await
    } else {
        // TODO head object before presigning? check commit history for some of that code.
        let req = GetObjectRequest {
            bucket: BUCKET.into(),
            key: pathstr.into(),
            ..Default::default()
        };
        let presigned = req.get_presigned_url(
            &REGION,
            &ctx.credentials,
            &PreSignedRequestOption {
                expires_in: Duration::from_secs(60 * 60 * 24),
            },
        );
        Ok(Box::new(warp::redirect::temporary(
            Uri::from_str(&presigned).map_err(RequestError::BadPresignedUrl)?,
        )))
    }
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .target(env_logger::Target::Stdout)
        .format_timestamp(None)
        .format_module_path(false)
        .init();
    let s3 = Arc::new(S3Client::new(REGION.clone()));
    let credentials = Arc::new(
        DefaultCredentialsProvider::new()
            .unwrap()
            .credentials()
            .await
            .unwrap(),
    );
    let mut handlebars = Handlebars::new();
    handlebars.set_strict_mode(true);
    handlebars
        .register_template_string("directory_listing", DIR_LIST_TEMPLATE)
        .expect("bad directory_listing template");
    let handlebars_arc = Arc::new(handlebars);
    let route = warp::path::full().and_then(move |path: FullPath| {
        request(
            path,
            Ctx {
                s3: s3.clone(),
                credentials: credentials.clone(),
                handlebars: handlebars_arc.clone(),
            },
        )
    });

    // TODO don't print errors in release
    // TODO access logging
    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;
}
