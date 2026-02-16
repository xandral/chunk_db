#[derive(Clone, Debug)]
pub enum FilterValue {
    Int(i64),
    UInt(u64),
    String(String),
    Bool(bool),
}

impl From<i64> for FilterValue {
    fn from(v: i64) -> Self {
        FilterValue::Int(v)
    }
}

impl From<i32> for FilterValue {
    fn from(v: i32) -> Self {
        FilterValue::Int(v as i64)
    }
}

impl From<u64> for FilterValue {
    fn from(v: u64) -> Self {
        FilterValue::UInt(v)
    }
}

impl From<&str> for FilterValue {
    fn from(v: &str) -> Self {
        FilterValue::String(v.to_string())
    }
}

impl From<String> for FilterValue {
    fn from(v: String) -> Self {
        FilterValue::String(v)
    }
}

impl From<bool> for FilterValue {
    fn from(v: bool) -> Self {
        FilterValue::Bool(v)
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Clone, Debug)]
pub struct Filter {
    pub column: String,
    pub op: FilterOp,
    pub value: FilterValue,
}

impl Filter {
    pub fn eq(column: &str, value: impl Into<FilterValue>) -> Self {
        Self {
            column: column.to_string(),
            op: FilterOp::Eq,
            value: value.into(),
        }
    }

    pub fn neq(column: &str, value: impl Into<FilterValue>) -> Self {
        Self {
            column: column.to_string(),
            op: FilterOp::NotEq,
            value: value.into(),
        }
    }

    pub fn gt(column: &str, value: i64) -> Self {
        Self {
            column: column.to_string(),
            op: FilterOp::Gt,
            value: value.into(),
        }
    }

    pub fn gte(column: &str, value: i64) -> Self {
        Self {
            column: column.to_string(),
            op: FilterOp::GtEq,
            value: value.into(),
        }
    }

    pub fn lt(column: &str, value: i64) -> Self {
        Self {
            column: column.to_string(),
            op: FilterOp::Lt,
            value: value.into(),
        }
    }

    pub fn lte(column: &str, value: i64) -> Self {
        Self {
            column: column.to_string(),
            op: FilterOp::LtEq,
            value: value.into(),
        }
    }

    pub fn between(column: &str, min: i64, max: i64) -> Vec<Self> {
        vec![Self::gte(column, min), Self::lte(column, max)]
    }

    pub fn and(self, other: Filter) -> CompositeFilter {
        CompositeFilter::And(vec![
            CompositeFilter::Single(self),
            CompositeFilter::Single(other),
        ])
    }

    pub fn or(self, other: Filter) -> CompositeFilter {
        CompositeFilter::Or(vec![
            CompositeFilter::Single(self),
            CompositeFilter::Single(other),
        ])
    }
}

impl From<Filter> for CompositeFilter {
    fn from(f: Filter) -> Self {
        CompositeFilter::Single(f)
    }
}

impl From<Vec<Filter>> for CompositeFilter {
    fn from(filters: Vec<Filter>) -> Self {
        CompositeFilter::And(filters.into_iter().map(CompositeFilter::Single).collect())
    }
}

#[derive(Clone, Debug)]
pub enum CompositeFilter {
    Single(Filter),
    And(Vec<CompositeFilter>),
    Or(Vec<CompositeFilter>),
}

impl CompositeFilter {
    pub fn and(filters: Vec<CompositeFilter>) -> Self {
        CompositeFilter::And(filters)
    }

    pub fn or(filters: Vec<CompositeFilter>) -> Self {
        CompositeFilter::Or(filters)
    }

    pub fn single(f: Filter) -> Self {
        CompositeFilter::Single(f)
    }

    /// Flatten to simple filters (for now, only handle AND)
    pub fn flatten(&self) -> Vec<Filter> {
        match self {
            CompositeFilter::Single(f) => vec![f.clone()],
            CompositeFilter::And(filters) => {
                filters.iter().flat_map(|cf| cf.flatten()).collect()
            }
            CompositeFilter::Or(_) => {
                // OR not fully supported in pruning yet - return empty to be safe
                vec![]
            }
        }
    }

    /// Check if this filter contains any OR operations
    pub fn has_or(&self) -> bool {
        match self {
            CompositeFilter::Single(_) => false,
            CompositeFilter::Or(_) => true,
            CompositeFilter::And(filters) => {
                filters.iter().any(|cf| cf.has_or())
            }
        }
    }

    pub fn normalize(self) -> Vec<Vec<Filter>> {
        match self {
            CompositeFilter::Single(f) => vec![vec![f]],
            CompositeFilter::Or(filters) => {
                // Simply collect all branches from children
                filters.into_iter()
                    .flat_map(|cf| cf.normalize())
                    .collect()
            }
            CompositeFilter::And(filters) => {
                // Cartesian product: Distribute AND over OR
                // Start with one empty branch
                let mut branches: Vec<Vec<Filter>> = vec![vec![]];

                for cf in filters {
                    let child_branches = cf.normalize();
                    let mut new_branches = Vec::new();

                    for existing in &branches {
                        for extension in &child_branches {
                            let mut combined = existing.clone();
                            combined.extend(extension.clone());
                            new_branches.push(combined);
                        }
                    }
                    branches = new_branches;
                }
                branches
            }
        }
    }
}