use env_logger;
use handlebars::{Handlebars, RenderError};
use lazy_static::lazy_static;
use log::warn;
use rusoto_core::{
    credential::{AwsCredentials, DefaultCredentialsProvider, ProvideAwsCredentials},
    request::BufferedHttpResponse,
    Region, RusotoError,
};
use rusoto_s3::{
    util::{PreSignedRequest, PreSignedRequestOption},
    GetObjectRequest, HeadObjectError, HeadObjectRequest, ListObjectsV2Request, S3Client, S3,
};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::Arc, time::Duration};
use warp::{
    http::uri::InvalidUri,
    hyper::{StatusCode, Uri},
    path::FullPath,
    reject::Reject,
    Filter,
};
mod utils;
use utils::{get_parent, path_to_key};

lazy_static! {
    static ref REGION: Region = Region::Custom {
        name: "local".to_string(),
        endpoint: "http://127.0.0.1:9000".to_string()
    };
}
const BUCKET: &'static str = "testbucket";

const DIR_LIST_TEMPLATE: &'static str = include_str!("directory_listing.hbs");

#[derive(Debug)]
struct BadPresignedUrl {
    inner: InvalidUri,
}
impl Reject for BadPresignedUrl {}

#[derive(Debug)]
struct S3Error<T> {
    inner: RusotoError<T>,
    path: String,
}
impl<T> Reject for S3Error<T> where T: std::fmt::Debug + Send + Sync + 'static {}

#[derive(Debug)]
struct TemplateError {
    inner: RenderError,
}
impl Reject for TemplateError {}

#[derive(Debug)]
struct UnknownError {
    message: &'static str,
}
impl Reject for UnknownError {}

struct Ctx {
    s3: Arc<S3Client>,
    credentials: Arc<AwsCredentials>,
    handlebars: Arc<Handlebars<'static>>,
}

#[derive(Deserialize)]
struct Query {
    nodir: Option<bool>,
}

#[derive(Serialize)]
struct DirectoryListingData<'a> {
    title: &'a str,
    path: &'a str,
    parent: &'a str,
    items: Vec<&'a str>,
}

async fn directory_listing(base: &str, ctx: Ctx) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let prefix = path_to_key(base)?;
    // TODO use pagination
    let list = ctx
        .s3
        .list_objects_v2(ListObjectsV2Request {
            bucket: BUCKET.into(),
            prefix: Some(prefix.into()),
            delimiter: Some("/".into()),
            // TODO use delimiter?
            ..Default::default()
        })
        .await
        .map_err(|e| {
            warp::reject::custom(S3Error {
                inner: e,
                path: base.into(),
            })
        })?;
    if list.is_truncated == Some(true) {
        warn!("list of ({}) has too many results", base);
    }
    let mut items = vec![];
    if let Some(ref common) = list.common_prefixes {
        items.extend(common.iter().filter_map(|c| {
            c.prefix.as_deref()?.strip_prefix(prefix)
        }));
    }
    if let Some(ref contents) = list.contents {
        items.extend(contents
            .iter()
            // TODO handle keys that end with /
            // TODO log None here
            .filter_map(|c| Some(c.key.as_deref()?.strip_prefix(prefix)?)));
    }
    let parent = get_parent(base).unwrap_or("");
    dbg!(&parent);
    let data = DirectoryListingData {
        // TODO change
        title: "title",
        path: base,
        parent,
        items,
    };
    Ok(Box::new(warp::reply::html(
        ctx.handlebars
            .render("directory_listing", &data)
            .map_err(|e| TemplateError { inner: e })?,
    )))
}

// TODO is there some way to avoid the box in the return?
async fn request(
    path: FullPath,
    query: Query,
    ctx: Ctx,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO unescape path?
    let pathstr = path.as_str();
    dbg!(pathstr);
    debug_assert!(pathstr.starts_with('/'));
    if pathstr.ends_with("/") && query.nodir != Some(true) {
        directory_listing(pathstr, ctx).await
    } else {
        let s3path = pathstr
            .strip_prefix('/')
            .ok_or_else(|| warp::reject::not_found())?;
        match ctx
            .s3
            .head_object(HeadObjectRequest {
                bucket: BUCKET.into(),
                key: s3path.into(),
                ..Default::default()
            })
            .await
        {
            Ok(_) => {
                // TODO handle glacier
                let req = GetObjectRequest {
                    bucket: BUCKET.into(),
                    key: s3path.into(),
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
                    Uri::from_str(&presigned).map_err(|e| BadPresignedUrl { inner: e })?,
                )))
            }
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_)))
            // bug in rusoto means NoSuchKey will not be returned if key doesn't exist (https://github.com/rusoto/rusoto/issues/716)
            // so we check manually
            | Err(RusotoError::Unknown(BufferedHttpResponse {
                status: StatusCode::NOT_FOUND,
                ..
            })) => {
                if query.nodir == Some(true) {
                    Err(warp::reject::not_found())
                } else {
                    let mut base = pathstr.to_string();
                    if !base.ends_with("/") {
                        base.push('/');
                    }
                    directory_listing(&base, ctx).await
                }
            }
            Err(e) => Err(warp::reject::custom(S3Error {
                inner: e,
                path: path.as_str().into(),
            })),
        }
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
    let route = warp::path::full().and(warp::query::<Query>()).and_then(
        move |path: FullPath, query: Query| {
            request(
                path,
                query,
                Ctx {
                    s3: s3.clone(),
                    credentials: credentials.clone(),
                    handlebars: handlebars_arc.clone(),
                },
            )
        },
    );

    // TODO don't print errors in release
    // TODO access logging
    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;
}
