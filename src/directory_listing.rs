use crate::utils::{get_parent, url_encode};
use handlebars::{Handlebars, RenderError};
use log::warn;
use rusoto_core::RusotoError;
use rusoto_s3::{ListObjectsV2Error, ListObjectsV2Request, S3Client, S3};
use serde::Serialize;
use thiserror::Error;
use warp::{reject::Reject, Rejection};

const DIR_LIST_TEMPLATE: &'static str = include_str!("directory_listing.hbs");

#[derive(Error, Debug)]
enum DirectoryListingError {
    #[error("template error")]
    TemplateError(#[from] RenderError),
    #[error("s3 error")]
    S3Error(#[from] RusotoError<ListObjectsV2Error>),
}
impl Reject for DirectoryListingError {}

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

pub struct DirectoryLister {
    handlebars: Handlebars<'static>,
}

impl DirectoryLister {
    pub fn new() -> Self {
        let mut handlebars = Handlebars::new();
        handlebars.set_strict_mode(true);
        handlebars
            .register_template_string("directory_listing", DIR_LIST_TEMPLATE)
            .expect("bad directory_listing template");
        DirectoryLister { handlebars }
    }

    pub async fn directory_listing(
        &self,
        base: &str,
        s3: &S3Client,
        bucket: &str,
    ) -> Result<Box<dyn warp::Reply>, Rejection> {
        debug_assert!(base.is_empty() || base.ends_with('/'));
        let mut dirs: Vec<String> = vec![];
        let mut files: Vec<String> = vec![];
        let mut continuation_token = None;
        loop {
            // TODO use pagination
            let list = s3
                .list_objects_v2(ListObjectsV2Request {
                    bucket: bucket.to_string(),
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
                self.handlebars
                    .render("directory_listing", &data)
                    .map_err(DirectoryListingError::TemplateError)?,
            )))
        }
    }
}
