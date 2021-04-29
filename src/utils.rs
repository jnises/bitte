use warp::reject::Reject;

pub fn get_parent(path: &str) -> Option<&str> {
    debug_assert!(path.starts_with("/"));

    if path.is_empty() {
        None
    } else {
        let path = path.trim_end_matches('/');
        if let Some(slashi) = path.rfind('/') {
            Some(&path[..slashi + 1])
        } else {
            Some("")
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct BadPathError {
    path: String,
}
impl Reject for BadPathError {}

pub fn path_to_key(path: &str) -> Result<&str, BadPathError> {
    debug_assert!(path.starts_with("/"));
    Ok(path
        .strip_prefix('/')
        .ok_or_else(|| BadPathError { path: path.into() })?)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn get_parent_test_0() {
        assert_eq!(get_parent("/"), None);
    }

    #[test]
    fn get_parent_test_1() {
        assert_eq!(get_parent("/asdf"), Some("/"));
    }

    #[test]
    fn get_parent_test_2() {
        assert_eq!(get_parent("/asdf/"), Some("/"));
    }

    #[test]
    fn get_parent_test_3() {
        assert_eq!(get_parent("/asdf/qwer"), Some("/asdf/"));
    }

    #[test]
    fn path_to_key_test_0() {
        assert_eq!(path_to_key("/asdf"), Ok("asdf"));
    }

    #[test]
    #[should_panic]
    fn path_to_key_test_1() {
        path_to_key("asdf").unwrap();
    }
}
