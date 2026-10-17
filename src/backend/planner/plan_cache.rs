//! Query Plan Cache & Prepared Statement Support.
//!
//! Avoids redundant AST parsing, semantic analysis, and physical plan generation
//! for repeated query forms (such as bulk INSERTs and repetitive SELECTs).

use std::collections::{HashMap, VecDeque};

use crate::backend::error::{RookError, RookResult};
use crate::catalog::Catalog;
use crate::types::value::DataValue;
use rook_ast::QueryPlan;
use rook_ast::logical::LogicalPlan;
use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};

pub(crate) const DEFAULT_CACHE_CAPACITY: usize = 512;

/// Normalize a SQL statement for plan-cache lookup: replace literals with
/// `$1, $2, ...` placeholders, extract the literal values in order, and
/// strip comments so textually different but semantically identical
/// statements share one cache entry.
///
/// Tokenization is delegated to sqlparser's `Tokenizer` (GenericDialect),
/// which understands `-- line` and `/* block */` comments, doubled-quote
/// escapes, and identifier boundaries. Text the tokenizer rejects (e.g. an
/// unterminated string literal) cannot parse either, so it is its own key,
/// unnormalized; the parser reports the syntax error downstream.
pub fn normalize_sql(sql: &str) -> (String, Vec<String>) {
    let (normalized, literals) = normalize_sql_literals(sql);
    let texts = literals.into_iter().map(Literal::into_text).collect();
    (normalized, texts)
}

/// [`normalize_sql`], keeping what kind of literal each parameter was, so
/// `5`, `'5'`, `NULL` and `'NULL'` stay distinct.
pub fn normalize_sql_literals(sql: &str) -> (String, Vec<Literal>) {
    match tokenize_for_normalization(sql) {
        Ok(tokens) => normalize_from_tokens(sql, &tokens),
        Err(_) => (sql.trim().to_string(), Vec::new()),
    }
}

/// A literal lifted out of a statement by [`normalize_sql_literals`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Literal {
    /// A number as written, sign included.
    Number(String),
    /// A string's content, quotes and escapes removed.
    Text(String),
    /// The digits of `X'...'`.
    Hex(String),
    Null,
}

impl Literal {
    /// The literal's text: what `normalize_sql` reports.
    pub fn into_text(self) -> String {
        match self {
            Literal::Number(s) | Literal::Text(s) | Literal::Hex(s) => s,
            Literal::Null => "NULL".to_string(),
        }
    }

    /// The literal as SQL source.
    fn to_sql(&self) -> String {
        match self {
            Literal::Number(n) => n.clone(),
            Literal::Text(t) => format!("'{}'", t.replace('\'', "''")),
            Literal::Hex(h) => format!("X'{}'", h),
            Literal::Null => "NULL".to_string(),
        }
    }

    /// The literal as a value of `column`. Unlike CSV text, `''` and
    /// `'NULL'` are strings; only the `NULL` keyword is NULL.
    fn to_value(&self, column: &crate::catalog::types::Column) -> RookResult<Option<DataValue>> {
        let text = match self {
            Literal::Null => return Ok(None),
            Literal::Number(s) | Literal::Text(s) | Literal::Hex(s) => s,
        };
        DataValue::parse_and_encode(&column.data_type, text)
            .and_then(|bytes| DataValue::from_bytes(&column.data_type, &bytes))
            .map(Some)
            .map_err(|e| e.with_context(format!("column '{}'", column.name)))
    }
}

/// Tokenize `sql` with sqlparser's `Tokenizer` under the project's
/// `GenericDialect`, keeping comments as whitespace tokens so the
/// normalizer can skip them explicitly.
fn tokenize_for_normalization(
    sql: &str,
) -> Result<Vec<Token>, sqlparser::tokenizer::TokenizerError> {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    tokenizer.tokenize()
}

/// Rebuild a canonical statement from sqlparser tokens: literals become
/// `$n` placeholders, comments are dropped, whitespace runs collapse to a
/// single space, and a trailing `;` is stripped.
///
/// Spacing is purely positional (independent of the source's whitespace):
/// tokens are separated by one space except after `(` / `.`, before
/// `)` / `,` / `;` / `.`, and before a `(` that directly follows a word
/// (function-call / `VALUES(...)` glue). A `-`/`+` in unary position and
/// immediately followed by a number folds into a signed literal, so
/// `VALUES (-10)` parameterizes as one `$1` with value `-10`.
fn normalize_from_tokens(sql: &str, tokens: &[Token]) -> (String, Vec<Literal>) {
    let mut normalized = String::with_capacity(sql.len());
    let mut params: Vec<Literal> = Vec::new();

    // Tokens are separator-irrelevant after comment stripping, so spacing is
    // decided per pair of adjacent emitted tokens.
    macro_rules! space_if_needed {
        ($tok:expr, $prev_was_word:expr) => {
            let glue_before = matches!(
                $tok,
                Token::RParen | Token::Comma | Token::SemiColon | Token::Period
            ) || (matches!($tok, Token::LParen) && $prev_was_word);
            let glue_after_prev = normalized.ends_with('(') || normalized.ends_with('.');
            if !normalized.is_empty() && !glue_after_prev && !glue_before {
                normalized.push(' ');
            }
        };
    }

    // Was the previous *significant* token a Word? (function-call glue)
    let mut prev_was_word = false;
    let mut i = 0usize;
    while i < tokens.len() {
        let token = &tokens[i];
        match token {
            // Comments (`-- …` and `/* … */`) are whitespace tokens in
            // sqlparser 0.61 (struct variants) and carry no semantics —
            // skipped entirely (the fix).
            Token::Whitespace(
                Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment { .. },
            ) => {
                i += 1;
            }

            // All other whitespace is irrelevant: spacing is positional.
            Token::Whitespace(_) | Token::EOF => i += 1,

            // Unary sign immediately followed by an unsigned number folds
            // into a single signed literal. `a - 1` (binary) has whitespace
            // between `-` and `1` at the token level, so it never folds;
            // `(-10)` after `(` / `,` / `=` / operators does.
            Token::Minus | Token::Plus
                if i + 1 < tokens.len()
                    && matches!(tokens[i + 1], Token::Number(_, false))
                    && !prev_was_word =>
            {
                let sign = if matches!(token, Token::Minus) {
                    "-"
                } else {
                    ""
                };
                if let Token::Number(value, _) = &tokens[i + 1] {
                    space_if_needed!(token, prev_was_word);
                    params.push(Literal::Number(format!("{}{}", sign, value)));
                    normalized.push('$');
                    normalized.push_str(&params.len().to_string());
                }
                i += 2;
                prev_was_word = false;
            }

            Token::Word(w) => {
                let is_predicate_or_constraint_null = {
                    let mut k = i;
                    let mut prev_word = None;
                    while k > 0 {
                        k -= 1;
                        match &tokens[k] {
                            Token::Whitespace(_) => continue,
                            Token::Word(pw) => {
                                prev_word = Some(pw.value.to_ascii_lowercase());
                                break;
                            }
                            _ => break,
                        }
                    }
                    matches!(prev_word.as_deref(), Some("is" | "not"))
                };

                if w.quote_style.is_none()
                    && w.value.eq_ignore_ascii_case("null")
                    && !is_predicate_or_constraint_null
                {
                    space_if_needed!(token, prev_was_word);
                    params.push(Literal::Null);
                    normalized.push('$');
                    normalized.push_str(&params.len().to_string());
                    prev_was_word = false;
                    i += 1;
                } else {
                    space_if_needed!(token, prev_was_word);
                    match w.quote_style {
                        // Quoted identifiers: restore the original quoting so
                        // downstream template parsing sees the same shape. They
                        // are identifiers, not literals — never parameterized.
                        Some('"') => {
                            normalized.push('"');
                            normalized.push_str(&w.value);
                            normalized.push('"');
                        }
                        Some('`') => {
                            normalized.push('`');
                            normalized.push_str(&w.value);
                            normalized.push('`');
                        }
                        Some('[') => {
                            normalized.push('[');
                            normalized.push_str(&w.value);
                            normalized.push(']');
                        }
                        _ => normalized.push_str(&w.value),
                    }
                    prev_was_word = true;
                    i += 1;
                }
            }

            // Numeric literal — the token value includes the sign when the
            // tokenizer reports the "long" (signed) form.
            Token::Number(value, _long) => {
                space_if_needed!(token, prev_was_word);
                params.push(Literal::Number(value.clone()));
                normalized.push('$');
                normalized.push_str(&params.len().to_string());
                prev_was_word = false;
                i += 1;
            }

            // String-ish literals: the token already holds the unquoted,
            // unescaped content (sqlparser folds '' → '). Parameterize.
            Token::SingleQuotedString(v)
            | Token::NationalStringLiteral(v)
            | Token::HexStringLiteral(v)
            | Token::EscapedStringLiteral(v) => {
                space_if_needed!(token, prev_was_word);
                params.push(if matches!(token, Token::HexStringLiteral(_)) {
                    Literal::Hex(v.clone())
                } else {
                    Literal::Text(v.clone())
                });
                normalized.push('$');
                normalized.push_str(&params.len().to_string());
                prev_was_word = false;
                i += 1;
            }

            // Double-quoted strings (non-identifier dialects): keep the
            // quoting — not a literal.
            Token::DoubleQuotedString(v) => {
                space_if_needed!(token, prev_was_word);
                normalized.push('"');
                normalized.push_str(v);
                normalized.push('"');
                prev_was_word = false;
                i += 1;
            }

            // Pre-existing positional placeholders pass through untouched
            // (the legacy scanner mangled these into `$$N`). A numbered
            // placeholder reserves its slot in `params` so subsequent
            // extracted literals keep consistent `$n` numbering.
            Token::Placeholder(p) => {
                space_if_needed!(token, prev_was_word);
                normalized.push_str(p);
                if let Ok(n) = p
                    .strip_prefix('$')
                    .ok_or(())
                    .and_then(|s| s.parse::<usize>().map_err(|_| ()))
                {
                    while params.len() < n {
                        params.push(Literal::Text(String::new())); // reserved, never consumed
                    }
                }
                prev_was_word = false;
                i += 1;
            }

            // Semicolon: strip when it only terminates the statement;
            // interior separators are kept (multi-statement scripts).
            Token::SemiColon => {
                let rest_is_noise = tokens[i + 1..]
                    .iter()
                    .all(|t| matches!(t, Token::Whitespace(_) | Token::EOF));
                if !rest_is_noise {
                    space_if_needed!(token, prev_was_word);
                    normalized.push(';');
                    prev_was_word = false;
                }
                i += 1;
            }

            // Operators and punctuation: sqlparser's `Display` reconstructs
            // the canonical text (e.g. `=`, `(`, `,`).
            _ => {
                space_if_needed!(token, prev_was_word);
                normalized.push_str(&token.to_string());
                prev_was_word = false;
                i += 1;
            }
        }
    }

    (normalized.trim().to_string(), params)
}

/// Cached fast-path template for an INSERT statement.
#[derive(Clone, Debug)]
pub struct CachedInsert {
    pub table: String,
    pub columns: Vec<String>,
    pub arity: usize,
    pub row_arities: Vec<usize>,
}

impl CachedInsert {
    /// Execute the insert using the cached template and extracted parameter values.
    pub fn execute(
        &self,
        catalog: &Catalog,
        db_name: &str,
        values: &[Literal],
    ) -> RookResult<usize> {
        let db = catalog
            .databases
            .get(db_name)
            .ok_or_else(|| RookError::NotFound {
                entity: "Database",
                name: db_name.to_string(),
            })?;
        let table = db
            .tables
            .get(&self.table)
            .ok_or_else(|| RookError::NotFound {
                entity: "Table",
                name: self.table.clone(),
            })?;

        let row_arities = if self.row_arities.is_empty() {
            vec![self.arity]
        } else {
            self.row_arities.clone()
        };

        // Validate explicit column names if provided
        let mut col_positions = Vec::with_capacity(self.columns.len());
        if !self.columns.is_empty() {
            for col_name in &self.columns {
                let pos = table
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| {
                        RookError::Internal(format!(
                            "Column '{}' does not exist in table '{}'",
                            col_name, self.table
                        ))
                    })?;
                col_positions.push(pos);
            }
        }

        // Validate row arities upfront before performing any insertions
        let expected_row_len = if self.columns.is_empty() {
            table.columns.len()
        } else {
            self.columns.len()
        };

        let total_values_expected: usize = row_arities.iter().sum();
        if values.len() != total_values_expected {
            return Err(RookError::TypeMismatch(format!(
                "Expected {} values for table '{}', got {}",
                total_values_expected,
                self.table,
                values.len()
            )));
        }

        for &row_len in &row_arities {
            if self.columns.is_empty() {
                if row_len != expected_row_len {
                    return Err(RookError::TypeMismatch(format!(
                        "INSERT row has {} value(s) but table '{}' has {} column(s)",
                        row_len, self.table, expected_row_len
                    )));
                }
            } else {
                if row_len != expected_row_len {
                    return Err(RookError::TypeMismatch(format!(
                        "INSERT row has {} value(s) but {} column(s) listed",
                        row_len, expected_row_len
                    )));
                }
            }
        }

        // Execute row by row against one resolved table.
        let target =
            crate::backend::executor::load_csv::InsertTarget::new(catalog, db_name, &self.table)?;
        let mut offset = 0;
        let mut inserted_count = 0;

        for &row_len in &row_arities {
            let row_values = &values[offset..offset + row_len];
            offset += row_len;

            // Columns the statement leaves out take their DEFAULT (or NULL).
            let row: Vec<Option<DataValue>> = if self.columns.is_empty() {
                table
                    .columns
                    .iter()
                    .zip(row_values)
                    .map(|(column, literal)| literal.to_value(column))
                    .collect::<RookResult<_>>()?
            } else {
                let mut row: Vec<_> = table
                    .columns
                    .iter()
                    .map(|c| c.constraints.default.clone())
                    .collect();
                for (i, &pos) in col_positions.iter().enumerate() {
                    row[pos] = row_values[i].to_value(&table.columns[pos])?;
                }
                row
            };

            target.insert_row(catalog, &row)?;
            inserted_count += 1;
        }

        Ok(inserted_count)
    }
}

/// A cached entry in the plan cache.
#[derive(Clone, Debug)]
pub enum PlanCacheEntry {
    /// An INSERT template; its values are supplied per execution.
    Insert(CachedInsert),
    /// A planned statement. The plan holds the statement's literals, so it
    /// serves only a statement with these same `literals`.
    Plan {
        plan: LogicalPlan,
        literals: Vec<Literal>,
    },
}

/// Process-wide Plan Cache.
pub struct PlanCache {
    entries: HashMap<(String, String), PlanCacheEntry>,
    order: VecDeque<(String, String)>,
    capacity: usize,
}

impl PlanCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn get(&self, db_name: &str, normalized_sql: &str) -> Option<PlanCacheEntry> {
        let key = (db_name.to_string(), normalized_sql.to_string());
        self.entries.get(&key).cloned()
    }

    pub fn insert(&mut self, db_name: &str, normalized_sql: &str, entry: PlanCacheEntry) {
        let key = (db_name.to_string(), normalized_sql.to_string());
        if let std::collections::hash_map::Entry::Occupied(mut occupied) =
            self.entries.entry(key.clone())
        {
            occupied.insert(entry);
            return;
        }

        if self.entries.len() >= self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.entries.remove(&oldest);
        }

        self.order.push_back(key.clone());
        self.entries.insert(key, entry);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

/// Invalidate all entries in the plan cache (called when DDL runs).
pub fn invalidate_plan_cache(engine: &crate::backend::engine::Engine) {
    crate::backend::engine::lock(&engine.plans).clear();
}

/// Look up an entry in the plan cache.
pub fn get_cached_plan(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    normalized_sql: &str,
) -> Option<PlanCacheEntry> {
    use crate::backend::cache::{CacheSite, CacheTier};
    let hit = if engine.cache_enabled(CacheTier::Plans) {
        crate::backend::engine::lock(&engine.plans).get(db_name, normalized_sql)
    } else {
        None
    };
    engine.cache_counters.record(CacheSite::Plan, hit.is_some());
    hit
}

/// Store an entry in the plan cache.
pub fn store_cached_plan(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    normalized_sql: &str,
    entry: PlanCacheEntry,
) {
    if !engine.cache_enabled(crate::backend::cache::CacheTier::Plans) {
        return;
    }
    crate::backend::engine::lock(&engine.plans).insert(db_name, normalized_sql, entry);
}

/// Parse a normalized INSERT statement template without external parser dependencies.
pub fn parse_insert_template(normalized_sql: &str, arity: usize) -> Option<CachedInsert> {
    let raw_tokens = tokenize_for_normalization(normalized_sql).ok()?;
    let tokens: Vec<&Token> = raw_tokens
        .iter()
        .filter(|t| !matches!(t, Token::Whitespace(_) | Token::EOF))
        .collect();

    if tokens.len() < 5 {
        return None;
    }

    match (tokens[0], tokens[1]) {
        (Token::Word(w1), Token::Word(w2))
            if w1.value.eq_ignore_ascii_case("insert") && w2.value.eq_ignore_ascii_case("into") => {
        }
        _ => return None,
    }

    let mut idx = 2;

    let table = match tokens.get(idx)? {
        Token::Word(w) => {
            idx += 1;
            w.value.clone()
        }
        _ => return None,
    };

    let mut cols = Vec::new();
    if let Some(Token::LParen) = tokens.get(idx) {
        idx += 1;
        loop {
            match tokens.get(idx)? {
                Token::Word(w) => {
                    cols.push(w.value.clone());
                    idx += 1;
                }
                _ => return None,
            }
            match tokens.get(idx)? {
                Token::Comma => {
                    idx += 1;
                }
                Token::RParen => {
                    idx += 1;
                    break;
                }
                _ => return None,
            }
        }
    }

    match tokens.get(idx)? {
        Token::Word(w) if w.value.eq_ignore_ascii_case("values") => {
            idx += 1;
        }
        _ => return None,
    }

    let mut row_arities = Vec::new();
    loop {
        match tokens.get(idx)? {
            Token::LParen => {
                idx += 1;
            }
            _ => return None,
        }
        let mut count = 0;
        if let Some(Token::RParen) = tokens.get(idx) {
            idx += 1;
        } else {
            loop {
                match tokens.get(idx)? {
                    Token::Placeholder(_)
                    | Token::Number(_, _)
                    | Token::SingleQuotedString(_)
                    | Token::NationalStringLiteral(_)
                    | Token::HexStringLiteral(_)
                    | Token::EscapedStringLiteral(_) => {
                        count += 1;
                        idx += 1;
                    }
                    Token::Word(w) if w.value.eq_ignore_ascii_case("null") => {
                        count += 1;
                        idx += 1;
                    }
                    Token::Minus | Token::Plus
                        if idx + 1 < tokens.len()
                            && matches!(tokens[idx + 1], Token::Number(_, _)) =>
                    {
                        count += 1;
                        idx += 2;
                    }
                    _ => return None,
                }
                match tokens.get(idx)? {
                    Token::Comma => {
                        idx += 1;
                    }
                    Token::RParen => {
                        idx += 1;
                        break;
                    }
                    _ => return None,
                }
            }
        }
        row_arities.push(count);

        if let Some(Token::Comma) = tokens.get(idx) {
            idx += 1;
            continue;
        } else if let Some(Token::SemiColon) = tokens.get(idx) {
            idx += 1;
            if idx == tokens.len() {
                break;
            } else {
                return None;
            }
        } else if idx == tokens.len() {
            break;
        } else {
            return None;
        }
    }

    if row_arities.is_empty() {
        return None;
    }

    if row_arities.iter().sum::<usize>() != arity {
        return None;
    }

    Some(CachedInsert {
        table,
        columns: cols,
        arity,
        row_arities,
    })
}

/// Execute an insert SQL query through the plan cache if possible.
/// Returns `Ok(Some(count))` if routed through the plan cache, or `Ok(None)` if not an insert.
pub fn execute_cached_insert(
    catalog: &Catalog,
    db_name: &str,
    sql: &str,
) -> RookResult<Option<usize>> {
    let (normalized, params) = normalize_sql_literals(sql);
    let key_matched =
        normalized.starts_with("INSERT INTO") || normalized.starts_with("insert into");

    if !key_matched {
        return Ok(None);
    }

    if let Some(PlanCacheEntry::Insert(cached_insert)) =
        get_cached_plan(catalog.engine(), db_name, &normalized)
    {
        return cached_insert.execute(catalog, db_name, &params).map(Some);
    }

    // Cache miss: construct template
    if let Some(cached_insert) = parse_insert_template(&normalized, params.len()) {
        let res = cached_insert.execute(catalog, db_name, &params)?;
        store_cached_plan(
            catalog.engine(),
            db_name,
            &normalized,
            PlanCacheEntry::Insert(cached_insert),
        );
        return Ok(Some(res));
    }

    // If fast template failed (e.g. complex expression), fall back to SQL parser if registered
    if let Ok(QueryPlan::Insert(ref ip)) = rook_parser::parse_sql(sql)
        && ip.source_select.is_none()
    {
        let cached_insert = CachedInsert {
            table: ip.table.clone(),
            columns: ip.columns.clone(),
            arity: params.len(),
            row_arities: ip.values.iter().map(|r| r.len()).collect(),
        };
        let res = cached_insert.execute(catalog, db_name, &params)?;
        store_cached_plan(
            catalog.engine(),
            db_name,
            &normalized,
            PlanCacheEntry::Insert(cached_insert),
        );
        return Ok(Some(res));
    }

    Ok(None)
}

/// Prepared Statement.
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    /// The statement with its literals replaced by `$1`, `$2`, ...
    pub sql_template: String,
    pub db_name: String,
    pub param_count: usize,
    pub entry: PlanCacheEntry,
}

impl PreparedStatement {
    /// Prepare a statement from a SQL string.
    pub fn prepare(catalog: &Catalog, db_name: &str, sql: &str) -> RookResult<Self> {
        let (normalized, literals) = normalize_sql_literals(sql);
        let prepared = |entry: PlanCacheEntry| Self {
            sql_template: normalized.clone(),
            db_name: db_name.to_string(),
            param_count: literals.len(),
            entry,
        };
        match get_cached_plan(catalog.engine(), db_name, &normalized) {
            Some(entry @ PlanCacheEntry::Insert(_)) => return Ok(prepared(entry)),
            Some(PlanCacheEntry::Plan {
                plan,
                literals: cached,
            }) if cached == literals => {
                return Ok(prepared(PlanCacheEntry::Plan {
                    plan,
                    literals: cached,
                }));
            }
            _ => {}
        }

        let entry = if let Some(cached_insert) = parse_insert_template(&normalized, literals.len())
        {
            PlanCacheEntry::Insert(cached_insert)
        } else {
            match rook_parser::parse_sql(sql)? {
                QueryPlan::Insert(ref ip) if ip.source_select.is_none() => {
                    PlanCacheEntry::Insert(CachedInsert {
                        table: ip.table.clone(),
                        columns: ip.columns.clone(),
                        arity: literals.len(),
                        row_arities: ip.values.iter().map(|r| r.len()).collect(),
                    })
                }
                plan => PlanCacheEntry::Plan {
                    plan: crate::backend::planner::plan_query(&plan, catalog, db_name)
                        .map_err(|e| RookError::Internal(e.to_string()))?,
                    literals: literals.clone(),
                },
            }
        };
        store_cached_plan(catalog.engine(), db_name, &normalized, entry.clone());
        Ok(prepared(entry))
    }

    /// Execute the prepared statement with `params` in place of its
    /// literals. Returns the rows inserted or produced.
    pub fn execute(&self, catalog: &Catalog, params: &[Literal]) -> RookResult<usize> {
        if params.len() != self.param_count {
            return Err(RookError::TypeMismatch(format!(
                "prepared statement takes {} parameter(s), got {}",
                self.param_count,
                params.len()
            )));
        }
        let rows = match &self.entry {
            PlanCacheEntry::Insert(ins) => return ins.execute(catalog, &self.db_name, params),
            PlanCacheEntry::Plan { plan, literals } if literals.as_slice() == params => {
                crate::backend::executor::physical::engine::execute_plan_collect(
                    plan,
                    catalog,
                    &self.db_name,
                )?
            }
            PlanCacheEntry::Plan { .. } => {
                // The plan holds the prepared literals: plan the statement
                // again with the new ones.
                let sql = bind_template(&self.sql_template, params)?;
                let plan = rook_parser::parse_sql(&sql)?;
                let logical = crate::backend::planner::plan_query(&plan, catalog, &self.db_name)
                    .map_err(|e| RookError::Internal(e.to_string()))?;
                crate::backend::executor::physical::engine::execute_plan_collect(
                    &logical,
                    catalog,
                    &self.db_name,
                )?
            }
        };
        Ok(rows.len())
    }
}

/// Whether `text` is a plain numeric literal: digits, at most one point, an
/// optional sign and exponent.
fn is_number(text: &str) -> bool {
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((m, e)) => (m, Some(e.strip_prefix(['+', '-']).unwrap_or(e))),
        None => (unsigned, None),
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let mantissa_ok = match mantissa.split_once('.') {
        Some((whole, frac)) => {
            (whole.is_empty() || digits(whole))
                && (frac.is_empty() || digits(frac))
                && !(whole.is_empty() && frac.is_empty())
        }
        None => digits(mantissa),
    };
    mantissa_ok && exponent.is_none_or(digits)
}

/// `template` with each `$n` placeholder replaced by `params[n - 1]` as SQL.
/// A number is checked to be one, so a parameter can never add SQL of its own.
fn bind_template(template: &str, params: &[Literal]) -> RookResult<String> {
    let tokens = tokenize_for_normalization(template)
        .map_err(|e| RookError::Internal(format!("prepared statement template: {}", e)))?;
    let mut sql = String::with_capacity(template.len());
    for token in &tokens {
        let Token::Placeholder(p) = token else {
            sql.push_str(&token.to_string());
            continue;
        };
        let param = p
            .strip_prefix('$')
            .and_then(|n| n.parse::<usize>().ok())
            .and_then(|n| params.get(n.checked_sub(1)?))
            .ok_or_else(|| RookError::Internal(format!("no value for parameter {}", p)))?;
        if let Literal::Number(n) = param
            && !is_number(n)
        {
            return Err(RookError::TypeMismatch(format!("'{}' is not a number", n)));
        }
        sql.push_str(&param.to_sql());
    }
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_sql_insert() {
        let sql = "INSERT INTO staff VALUES (1, 'user_000001', 100);";
        let (norm, params) = normalize_sql(sql);
        assert_eq!(norm, "INSERT INTO staff VALUES($1, $2, $3)");
        assert_eq!(params, vec!["1", "user_000001", "100"]);
    }

    #[test]
    fn test_normalize_sql_select() {
        let sql = "SELECT id, name FROM staff WHERE id = 42";
        let (norm, params) = normalize_sql(sql);
        assert_eq!(norm, "SELECT id, name FROM staff WHERE id = $1");
        assert_eq!(params, vec!["42"]);
    }

    #[test]
    fn test_normalize_sql_count_star() {
        let sql = "SELECT COUNT(*) FROM staff";
        let (norm, params) = normalize_sql(sql);
        assert_eq!(norm, "SELECT COUNT(*) FROM staff");
        assert!(params.is_empty());
    }

    #[test]
    fn test_normalize_sql_strings_with_quotes() {
        let sql = "INSERT INTO users VALUES ('O''Reilly', -10)";
        let (norm, params) = normalize_sql(sql);
        assert_eq!(norm, "INSERT INTO users VALUES($1, $2)");
        assert_eq!(params, vec!["O'Reilly", "-10"]);
    }

    #[test]
    fn test_normalize_sql_strips_line_comment() {
        // Digits inside a line comment must NOT become parameters.
        let (norm, params) = normalize_sql("SELECT id FROM staff -- lookup user 42\nWHERE id = 7");
        assert_eq!(norm, "SELECT id FROM staff WHERE id = $1");
        assert_eq!(params, vec!["7"]);
    }

    #[test]
    fn test_normalize_sql_strips_block_comment() {
        let (norm, params) =
            normalize_sql("SELECT /* version 2, tuning 'quotes' 123 */ id FROM staff WHERE id = 5");
        assert_eq!(norm, "SELECT id FROM staff WHERE id = $1");
        assert_eq!(params, vec!["5"]);
    }

    #[test]
    fn test_normalize_sql_comment_variants_share_cache_key() {
        // Semantically identical statements with different comments (or no
        // comments) must normalize to the same key — this is the whole point
        // of comment handling.
        let a = normalize_sql("SELECT * FROM t WHERE x = 1 /* fast path */").0;
        let b = normalize_sql("SELECT * FROM t -- slow path\nWHERE x = 2").0;
        let c = normalize_sql("SELECT * FROM t WHERE x = 3").0;
        assert_eq!(a, b);
        assert_eq!(b, c);
    }

    #[test]
    fn test_normalize_sql_comment_between_tokens_separates() {
        // A comment where a space would be must not glue tokens together.
        let (norm, params) = normalize_sql("SELECT a/*c*/+1 FROM t");
        assert_eq!(norm, "SELECT a + $1 FROM t");
        assert_eq!(params, vec!["1"]);
    }

    #[test]
    fn test_normalize_sql_string_containing_comment_markers() {
        // Comment markers INSIDE a string literal are string content, not
        // comments — the literal is parameterized, nothing is stripped.
        let (norm, params) = normalize_sql("SELECT * FROM t WHERE s = 'has -- dashes'");
        assert_eq!(norm, "SELECT * FROM t WHERE s = $1");
        assert_eq!(params, vec!["has -- dashes"]);
    }

    #[test]
    fn test_normalize_sql_numbered_placeholder_preserved() {
        // Pre-existing positional placeholders survive (legacy scanner
        // mangled `$1` into `$$1`) and reserve their slot so extracted
        // literals keep consistent numbering. The empty string is the
        // reserved slot — placeholders are never substituted by the cache.
        let (norm, params) = normalize_sql("SELECT * FROM t WHERE id = $1 AND n = 2");
        assert_eq!(norm, "SELECT * FROM t WHERE id = $1 AND n = $2");
        assert_eq!(params, vec!["", "2"]);
    }

    #[test]
    fn test_normalize_sql_whitespace_insensitive() {
        let a = normalize_sql("SELECT  id,name  FROM  t").0;
        let b = normalize_sql("SELECT id, name FROM t").0;
        assert_eq!(a, b);
    }

    #[test]
    fn test_normalize_sql_keyword_case_preserved() {
        // The normalizer intentionally does not case-fold; case-variant SQL
        // simply occupies two cache slots (same behavior as before).
        let a = normalize_sql("select * from t where x = 1").0;
        let b = normalize_sql("SELECT * FROM t WHERE x = 1").0;
        assert_ne!(a, b);
    }

    #[test]
    fn test_normalize_sql_unterminated_string_falls_back() {
        // The tokenizer rejects the unterminated literal: the text is its
        // own key, not a panic.
        let (norm, _params) = normalize_sql("SELECT 'oops FROM t");
        assert!(norm.starts_with("SELECT"));
    }
}
