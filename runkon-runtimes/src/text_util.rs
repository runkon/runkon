use crate::error::{Result, RuntimeError};

/// Validate that a `run_id` is safe to use as a filesystem path component.
///
/// Rejects empty strings, path separators, and any character outside
/// `[A-Za-z0-9\-_]` to prevent path-traversal attacks.
pub fn validate_run_id(run_id: &str) -> Result<()> {
    if !run_id.is_empty()
        && run_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        Ok(())
    } else {
        Err(RuntimeError::InvalidInput(format!(
            "invalid run_id '{run_id}': must be non-empty and contain only alphanumeric characters, hyphens, or underscores"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_valid(run_id: &str) {
        validate_run_id(run_id)
            .unwrap_or_else(|e| panic!("expected '{run_id}' to be valid, got error: {e}"));
    }

    fn assert_invalid(run_id: &str) {
        let err = validate_run_id(run_id)
            .err()
            .unwrap_or_else(|| panic!("expected '{run_id}' to be rejected"));
        assert!(
            matches!(err, RuntimeError::InvalidInput(_)),
            "expected InvalidInput for '{run_id}', got: {err:?}"
        );
    }

    #[test]
    fn accepts_alphanumeric() {
        assert_valid("abc123");
        assert_valid("RUN42");
    }

    #[test]
    fn accepts_hyphens_and_underscores() {
        assert_valid("run-id_42");
        assert_valid("a-b-c");
        assert_valid("a_b_c");
    }

    #[test]
    fn accepts_typical_ulid_shape() {
        // ULIDs are Crockford base32 (uppercase alphanumeric, 26 chars).
        assert_valid("01H7QRABCDEF1234567890ABCD");
    }

    #[test]
    fn rejects_empty() {
        assert_invalid("");
    }

    #[test]
    fn rejects_path_traversal() {
        assert_invalid("../etc/passwd");
        assert_invalid("..");
        assert_invalid("a/../b");
    }

    #[test]
    fn rejects_path_separators() {
        assert_invalid("foo/bar");
        assert_invalid("foo\\bar");
    }

    #[test]
    fn rejects_shell_metacharacters() {
        assert_invalid("foo;rm");
        assert_invalid("foo|bar");
        assert_invalid("foo&bar");
        assert_invalid("foo$bar");
        assert_invalid("foo`bar`");
        assert_invalid("foo bar");
    }

    #[test]
    fn rejects_null_byte() {
        assert_invalid("foo\0bar");
    }

    #[test]
    fn rejects_leading_dot() {
        // Leading-dot files are filesystem-special; '.' isn't in the allowed set.
        assert_invalid(".hidden");
        assert_invalid(".");
    }

    #[test]
    fn rejects_non_ascii_lookalikes() {
        // Cyrillic а (U+0430) — visually identical to ASCII 'a'
        assert_invalid("\u{0430}bc");
        // Greek ο (U+03BF) — visually identical to ASCII 'o'
        assert_invalid("f\u{03BF}o");
        // Mathematical bold 𝟎 (U+1D7CE) — visually identical to ASCII '0'
        assert_invalid("\u{1D7CE}123");
        // Han ideograph 中 (U+4E2D)
        assert_invalid("\u{4E2D}run");
    }
}
