use env_logger;
use handlebars::{Handlebars, RenderError};
use log::warn;
use rusoto_core::{
    credential::{AwsCredentials, DefaultCredentialsProvider, ProvideAwsCredentials},
    Region, RusotoError,
};
use rusoto_s3::{
    util::{PreSignedRequest, PreSignedRequestOption},
    GetObjectRequest, HeadObjectError, ListObjectsV2Error, ListObjectsV2Request, S3Client, S3,
};
use serde::Serialize;
use std::{convert::Infallible, str::FromStr, sync::Arc, time::Duration};
use structopt::StructOpt;
use thiserror::Error;
use warp::{
    http::uri::InvalidUri,
    hyper::{StatusCode, Uri},
    path::FullPath,
    reject::Reject,
    Filter, Rejection,
};
mod utils;
use utils::{get_parent, url_encode};

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
    bucket: Arc<String>,
    region: Arc<Region>,
    credentials: Arc<AwsCredentials>,
    handlebars: Arc<Handlebars<'static>>,
}

// TODO move directory listing stuff to separate file
#[derive(Serialize)]
struct DirectoryListingItem {
    name: String,
    url: String,
}

#[derive(Serialize)]
struct DirectoryListingData<'a> {
    title: &'a str,
    path: &'a str,
    parent: &'a str,
    items: Vec<DirectoryListingItem>,
}

async fn directory_listing(base: &str, ctx: &Ctx) -> Result<Box<dyn warp::Reply>, Rejection> {
    debug_assert!(base.is_empty() || base.ends_with('/'));
    let mut dirs: Vec<String> = vec![];
    let mut files: Vec<String> = vec![];
    let mut continuation_token = None;
    loop {
        // TODO use pagination
        let list = ctx
            .s3
            .list_objects_v2(ListObjectsV2Request {
                bucket: (*ctx.bucket).clone(),
                prefix: Some(base.into()),
                delimiter: Some("/".into()),
                continuation_token: continuation_token.take(),
                ..Default::default()
            })
            .await
            .map_err(DirectoryListingError::S3Error)?;
        continuation_token = list.next_continuation_token;
        if let Some(common) = list.common_prefixes {
            dirs.extend(common.into_iter().filter_map(|c| {
                let p = c.prefix.or_else(|| {
                    warn!("none in s3 listing common_prefixes");
                    None
                })?;
                p.strip_prefix(base).map(Into::into).or_else(|| {
                    warn!("common prefix without expected prefix found ({})", p);
                    None
                })
            }));
        }
        if let Some(contents) = list.contents {
            files.extend(contents.into_iter().filter_map(|c| -> Option<String> {
                let key = c.key.or_else(|| {
                    warn!("none key in s3 listing contents");
                    None
                })?;
                if key.ends_with('/') {
                    warn!("key ending with / found ({})", key);
                    return None;
                }
                key.strip_prefix(base).map(Into::into).or_else(|| {
                    warn!("key without expected prefix found ({})", key);
                    None
                })
            }));
        }
        if continuation_token.is_none() {
            break;
        }
    }
    if dirs.is_empty() && files.is_empty() {
        Err(warp::reject::not_found())
    } else {
        let get_url = |name: &str| url_encode(&format!("/{}{}", base, name));
        let mut items = Vec::with_capacity(dirs.len() + files.len());
        items.extend(dirs.into_iter().map(|name| {
            let url = get_url(&name);
            DirectoryListingItem { name, url }
        }));
        items.extend(files.into_iter().map(|name| {
            let url = get_url(&name);
            DirectoryListingItem { name, url }
        }));
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
async fn request(path: FullPath, ctx: Ctx) -> Result<Box<dyn warp::Reply>, Rejection> {
    let pathstr = &percent_encoding::percent_decode_str(path.as_str())
        .decode_utf8()
        .map_err(RequestError::EncodingError)?;
    let pathstr = pathstr
        .strip_prefix("/")
        .ok_or_else(|| warp::reject::not_found())?;
    if pathstr.is_empty() || pathstr.ends_with("/") {
        directory_listing(pathstr, &ctx).await
    } else {
        // TODO head object before presigning? check commit history for some of that code.
        let req = GetObjectRequest {
            bucket: (*ctx.bucket).clone(),
            key: pathstr.into(),
            ..Default::default()
        };
        let presigned = req.get_presigned_url(
            &ctx.region,
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

async fn handle_errors(e: Rejection) -> Result<impl warp::Reply, Infallible> {
    let code;
    let message;

    if e.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = "NOT_FOUND";
    } else if let Some(_) = e.find::<warp::reject::MethodNotAllowed>() {
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "METHOD_NOT_ALLOWED";
    } else {
        warn!("unhandled rejection: {:?}", e);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "UNHANDLED_ERROR";
    }

    Ok(warp::reply::with_status(message, code))
}

#[derive(StructOpt, Debug)]
#[structopt(name = "bitte")]
struct Opt {
    #[structopt(long)]
    bucket: String,

    #[structopt(long)]
    region: Option<String>,

    #[structopt(long)]
    endpoint: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .target(env_logger::Target::Stdout)
        .format_timestamp(None)
        .format_module_path(false)
        .init();
    let opt = Opt::from_args();
    let region = if let Some(endpoint) = opt.endpoint {
        Region::Custom {
            name: opt.region.unwrap_or_else(|| "custom".into()),
            endpoint,
        }
    } else {
        if let Some(region) = opt.region {
            Region::from_str(&region).expect("bad region provided")
        } else {
            Region::default()
        }
    };
    let s3 = Arc::new(S3Client::new(region.clone()));
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
    let region_arc = Arc::new(region);
    let bucket_arc = Arc::new(opt.bucket);
    let route = warp::path::full()
        .and_then(move |path: FullPath| {
            request(
                path,
                Ctx {
                    s3: s3.clone(),
                    bucket: bucket_arc.clone(),
                    region: region_arc.clone(),
                    credentials: credentials.clone(),
                    handlebars: handlebars_arc.clone(),
                },
            )
        })
        .recover(handle_errors);

    // TODO access logging
    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;
}
