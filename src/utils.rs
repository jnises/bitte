pub fn get_parent(path: &str) -> Option<&str> {
    if path.is_empty() {
        None
    } else {
        let path = path.trim_end_matches('/');
        if let Some(slashi) = path.rfind('/') {
            Some(&path[..slashi+1])
        } else {
            Some("")
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn get_parent_test_0() {
        assert_eq!(get_parent(""), None);
    }

    #[test]
    fn get_parent_test_1() {
        assert_eq!(get_parent("asdf"), Some(""));
    }

    #[test]
    fn get_parent_test_2() {
        assert_eq!(get_parent("asdf/"), Some(""));
    }

    #[test]
    fn get_parent_test_3() {
        assert_eq!(get_parent("asdf/qwer"), Some("asdf/"));
    }
}