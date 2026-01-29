/// Parsuje pattern expression s podporou OR a AND NOT operátorů.
///
/// # Podporované formáty
/// - `public, user_*` - OR pomocí čárky
/// - `public OR user_*` - OR pomocí klíčového slova
/// - `user_* -test` - AND NOT pomocí minusu
/// - `user_* AND NOT test` - AND NOT pomocí klíčového slova
/// - `public, user_* -test -tmp` - kombinace
///
/// # Příklady
/// ```
/// let (includes, excludes) = parse_pattern_expression("public, user_* -test");
/// assert_eq!(includes, vec!["public", "user_*"]);
/// assert_eq!(excludes, vec!["test"]);
/// ```
///
/// # Returns
/// Vrací tuple `(includes, excludes)` - oba jsou Vec<String>
pub fn parse_pattern_expression(input: &str) -> (Vec<String>, Vec<String>) {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return (vec!["*".to_string()], Vec::new());
    }

    let mut includes: Vec<String> = Vec::new();
    let mut excludes: Vec<String> = Vec::new();
    let mut neg_next = false;

    for token in trimmed.replace(',', " , ").split_whitespace() {
        if token == "," || token.eq_ignore_ascii_case("or") || token.eq_ignore_ascii_case("and") {
            continue;
        }
        if token.eq_ignore_ascii_case("not") || token == "-" {
            neg_next = true;
            continue;
        }

        let mut value = token;
        let mut is_exclude = neg_next;
        if value.starts_with('-') {
            is_exclude = true;
            value = &value[1..];
        }
        if value.is_empty() {
            neg_next = false;
            continue;
        }

        if is_exclude {
            excludes.push(value.to_string());
        } else {
            includes.push(value.to_string());
        }
        neg_next = false;
    }

    if includes.is_empty() {
        includes.push("*".to_string());
    }

    (includes, excludes)
}

/// Testuje, zda název matchuje wildcard pattern.
///
/// # Podporované wildcards
/// - `*` - matchuje libovolný počet znaků
/// - `user_*` - matchuje user_test, user_admin, atd.
/// - `*_audit` - matchuje table_audit, user_audit, atd.
/// - `*temp*` - matchuje temporary, tempdata, atd.
///
/// # Příklady
/// ```
/// assert!(matches_pattern("user_test", "user_*"));
/// assert!(matches_pattern("table_audit", "*_audit"));
/// assert!(matches_pattern("temporary", "*temp*"));
/// assert!(!matches_pattern("admin", "user_*"));
/// ```
pub fn matches_pattern(name: &str, pattern: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.is_empty() {
        return name == pattern;
    }

    let mut current_pos = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 && !pattern.starts_with('*') {
            if !name.starts_with(part) {
                return false;
            }
            current_pos = part.len();
            continue;
        }
        if i == parts.len() - 1 && !pattern.ends_with('*') {
            return name.ends_with(part);
        }
        if let Some(pos) = name[current_pos..].find(part) {
            current_pos += pos + part.len();
        } else {
            return false;
        }
    }

    true
}

/// Filtruje seznam názvů podle pattern expression.
///
/// # Příklady
/// ```
/// let names = vec!["public", "user_test", "user_admin", "temp_data"];
/// let filtered = filter_names(&names, "user_*");
/// assert_eq!(filtered, vec!["user_test", "user_admin"]);
///
/// let filtered2 = filter_names(&names, "user_* -test");
/// assert_eq!(filtered2, vec!["user_admin"]);
/// ```
pub fn filter_names(names: &[String], expression: &str) -> Vec<String> {
    let (includes, excludes) = parse_pattern_expression(expression);

    names
        .iter()
        .filter(|name| {
            // Musí matchovat alespoň jeden include pattern
            let matches_include = includes.iter().any(|pattern| matches_pattern(name, pattern));
            if !matches_include {
                return false;
            }

            // Nesmí matchovat žádný exclude pattern
            let matches_exclude = excludes.iter().any(|pattern| matches_pattern(name, pattern));
            !matches_exclude
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pattern_expression_simple() {
        let (includes, excludes) = parse_pattern_expression("public");
        assert_eq!(includes, vec!["public"]);
        assert_eq!(excludes.len(), 0);
    }

    #[test]
    fn test_parse_pattern_expression_or() {
        let (includes, excludes) = parse_pattern_expression("public, user_*");
        assert_eq!(includes, vec!["public", "user_*"]);
        assert_eq!(excludes.len(), 0);

        let (includes2, excludes2) = parse_pattern_expression("public OR user_*");
        assert_eq!(includes2, vec!["public", "user_*"]);
        assert_eq!(excludes2.len(), 0);
    }

    #[test]
    fn test_parse_pattern_expression_and_not() {
        let (includes, excludes) = parse_pattern_expression("user_* -test");
        assert_eq!(includes, vec!["user_*"]);
        assert_eq!(excludes, vec!["test"]);

        let (includes2, excludes2) = parse_pattern_expression("user_* AND NOT test");
        assert_eq!(includes2, vec!["user_*"]);
        assert_eq!(excludes2, vec!["test"]);
    }

    #[test]
    fn test_parse_pattern_expression_complex() {
        let (includes, excludes) = parse_pattern_expression("public, user_* -test -tmp");
        assert_eq!(includes, vec!["public", "user_*"]);
        assert_eq!(excludes, vec!["test", "tmp"]);
    }

    #[test]
    fn test_matches_pattern_exact() {
        assert!(matches_pattern("public", "public"));
        assert!(!matches_pattern("public", "user"));
    }

    #[test]
    fn test_matches_pattern_wildcard_end() {
        assert!(matches_pattern("user_test", "user_*"));
        assert!(matches_pattern("user_admin", "user_*"));
        assert!(!matches_pattern("admin", "user_*"));
    }

    #[test]
    fn test_matches_pattern_wildcard_start() {
        assert!(matches_pattern("table_audit", "*_audit"));
        assert!(matches_pattern("user_audit", "*_audit"));
        assert!(!matches_pattern("audit_table", "*_audit"));
    }

    #[test]
    fn test_matches_pattern_wildcard_middle() {
        assert!(matches_pattern("temporary", "*temp*"));
        assert!(matches_pattern("temp_data", "*temp*"));
        assert!(matches_pattern("data_temp_table", "*temp*"));
        assert!(!matches_pattern("data", "*temp*"));
    }

    #[test]
    fn test_filter_names() {
        let names = vec![
            "public".to_string(),
            "user_test".to_string(),
            "user_admin".to_string(),
            "temp_data".to_string(),
        ];

        let filtered = filter_names(&names, "user_*");
        assert_eq!(filtered, vec!["user_test", "user_admin"]);

        let filtered2 = filter_names(&names, "user_* -test");
        assert_eq!(filtered2, vec!["user_admin"]);

        let filtered3 = filter_names(&names, "public, temp_*");
        assert_eq!(filtered3, vec!["public", "temp_data"]);
    }
}
