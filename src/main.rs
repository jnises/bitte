use env_logger;
use handlebars::{Handlebars, RenderError};
use lazy_static::lazy_static;
use log::{debug, error, info, warn};
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
use std::{borrow::Borrow, str::FromStr, sync::Arc, time::Duration};
use warp::{
    http::uri::InvalidUri,
    hyper::{StatusCode, Uri},
    path::FullPath,
    Filter,
};
mod utils;
use utils::get_parent;

lazy_static! {
    static ref REGION: Region = Region::Custom {
        name: "local".to_string(),
        endpoint: "http://127.0.0.1:9000".to_string()
    };
}
const BUCKET: &'static str = "testbucket";

const DIR_LIST_TEMPLATE: &'static str = include_str!("directory_listing.hbs");

struct Ctx {
    s3: Arc<S3Client>,
    credentials: Arc<AwsCredentials>,
    handlebars: Arc<Handlebars<'static>>,
}

#[derive(Debug)]
struct BadPresignedUrl {
    inner: InvalidUri,
}
impl warp::reject::Reject for BadPresignedUrl {}

#[derive(Debug)]
struct S3Error<T> {
    inner: RusotoError<T>,
    path: String,
}
impl<T> warp::reject::Reject for S3Error<T> where T: std::fmt::Debug + Send + Sync + 'static {}

#[derive(Debug)]
struct TemplateError {
    inner: RenderError,
}
impl warp::reject::Reject for TemplateError {}

#[derive(Debug)]
struct UnknownError {
    message: &'static str,
}
impl warp::reject::Reject for UnknownError {}

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

async fn directory_listing(
    prefix: &str,
    ctx: Ctx,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO use pagination
    let list = ctx
        .s3
        .list_objects_v2(ListObjectsV2Request {
            bucket: BUCKET.into(),
            prefix: Some(prefix.into()),
            // TODO use delimiter?
            ..Default::default()
        })
        .await
        .map_err(|e| {
            warp::reject::custom(S3Error {
                inner: e,
                path: prefix.into(),
            })
        })?;
    if list.is_truncated == Some(true) {
        warn!("list of prefix ({}) has too many results", prefix);
    }
    match list.contents {
        Some(contents) => {
            if contents.is_empty() {
                Err(warp::reject::not_found())
            } else {
                let parent = if let Some(parent) = get_parent(prefix) { format!("/{}", parent) } else { "".into() };
                dbg!(prefix);
                dbg!(&parent);
                let data = DirectoryListingData {
                    // TODO change
                    title: "title",
                    path: &format!("/{}", prefix),
                    parent: &parent,
                    items: contents
                        .iter()
                        // TODO log None here
                        .filter_map(|c| Some(c.key.as_deref()?.strip_prefix(prefix)?))
                        .collect(),
                };
                Ok(Box::new(warp::reply::html(
                    ctx.handlebars
                        .render("directory_listing", &data)
                        .map_err(|e| TemplateError { inner: e })?,
                )))
            }
        }
        None => Err(warp::reject::not_found()),
    }
}

// TODO is there some way to avoid the box in the return?
async fn request(
    path: FullPath,
    query: Query,
    ctx: Ctx,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let s3path = path
        .as_str()
        .strip_prefix('/')
        .ok_or_else(|| UnknownError {
            message: "path does not start with /",
        })?;
    if path.as_str().ends_with("/") && query.nodir != Some(true) {
        directory_listing(s3path, ctx).await
    } else {
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
                    let mut prefix = s3path.to_string();
                    if !prefix.ends_with("/") {
                        prefix.push('/');
                    }
                    directory_listing(&prefix, ctx).await
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
