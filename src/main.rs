#![warn(clippy::all)]
use env_logger;
use log::error;
use rusoto_core::{
    credential::{AwsCredentials, DefaultCredentialsProvider, ProvideAwsCredentials},
    Region, RusotoError,
};
use rusoto_s3::{
    util::{PreSignedRequest, PreSignedRequestOption},
    GetObjectRequest, HeadObjectError, S3Client,
};
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
mod directory_listing;
mod utils;
use directory_listing::DirectoryLister;

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
    s3: S3Client,
    bucket: String,
    region: Region,
    credentials: AwsCredentials,
    lister: DirectoryLister,
}

// TODO is there some way to avoid the box in the return?
async fn request(path: FullPath, ctx: Arc<Ctx>) -> Result<Box<dyn warp::Reply>, Rejection> {
    let pathstr = &percent_encoding::percent_decode_str(path.as_str())
        .decode_utf8()
        .map_err(RequestError::EncodingError)?;
    let pathstr = pathstr
        .strip_prefix("/")
        .ok_or_else(|| warp::reject::not_found())?;
    if pathstr.is_empty() || pathstr.ends_with("/") {
        ctx.lister
            .directory_listing(pathstr, &ctx.s3, &ctx.bucket)
            .await
    } else {
        // TODO head object before presigning? check commit history for some of that code.
        let req = GetObjectRequest {
            bucket: ctx.bucket.clone(),
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
        error!("unhandled rejection: {:?}", e);
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
    let s3 = S3Client::new(region.clone());
    let credentials = DefaultCredentialsProvider::new()
        .unwrap()
        .credentials()
        .await
        .unwrap();
    let lister = DirectoryLister::new();
    let bucket = opt.bucket;
    let ctx = Arc::new(Ctx {
        s3,
        bucket,
        region,
        credentials,
        lister,
    });
    let route = warp::path::full()
        .and_then(move |path: FullPath| request(path, ctx.clone()))
        .recover(handle_errors);

    // TODO access logging
    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;
}
