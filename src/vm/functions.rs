//! SQL runtime functions module.

pub(super) fn like(left: &str, right: &str) -> bool {
    fn helper(left: &str, left_idx: usize, right: &str, right_idx: usize) -> bool {
        let mut left_chars = left[left_idx..].chars();
        let mut right_chars = right[right_idx..].chars();

        while let Some(pattern_char) = right_chars.next() {
            match pattern_char {
                '%' => {
                    // trailing % matches everything
                    if right_chars.as_str().is_empty() {
                        return true;
                    }
                    let remaining_pattern = right_chars.as_str();
                    let mut remaining_left = left_chars.as_str();

                    while !remaining_left.is_empty() {
                        if helper(remaining_left, 0, remaining_pattern, 0) {
                            return true;
                        }
                        if let Some(c) = remaining_left.chars().next() {
                            remaining_left = &remaining_left[c.len_utf8()..];
                        }
                    }
                    return false;
                }
                '_' => {
                    // _ matches exactly one character
                    if left_chars.next().is_none() {
                        return false;
                    }
                }
                _ => {
                    // exact match
                    if left_chars.next() != Some(pattern_char) {
                        return false;
                    }
                }
            }
        }

        left_chars.next().is_none()
    }

    helper(left, 0, right, 0)
}

pub(super) fn substring(string: &str, start: Option<usize>, count: Option<isize>) -> String {
    let substring_len = string.chars().count();
    let start = start.unwrap_or(1).saturating_sub(1); // sql use 1-based index
    let length = match count {
        Some(n) if n >= 0 => n as usize,
        _ => substring_len.saturating_sub(start),
    };

    string.chars().skip(start).take(length).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_like() {
        assert!(like("hello", "h%"));
        assert!(!like("hello", "H%"));
        assert!(!like("a", ""));
        assert!(like("", ""));
        assert!(like("Albert", "%er%"));
        assert!(like("Bernard", "%er%"));
        assert!(!like("Albert", "_er%"));
        assert!(like("Cheryl", "_her%"));
        assert!(!like("abc", "c"));
    }

    #[test]
    fn test_substring() {
        assert_eq!(substring("PostgreSQL", None, Some(8)), "PostgreS");
        assert_eq!(substring("PostgreSQL", Some(8), None), "SQL");
    }
}
