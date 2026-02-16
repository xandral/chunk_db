use crate::config::table_config::TableConfig;
use std::collections::HashMap;

/// Maps column names to their group indices
#[derive(Clone, Debug)]
pub struct ColumnGroupMapper {
    column_to_group: HashMap<String, u16>,
    group_to_columns: Vec<Vec<String>>,
    num_groups: u16,
}

impl ColumnGroupMapper {
    pub fn new(config: &TableConfig) -> Self {
        let mut column_to_group = HashMap::new();
        let mut group_to_columns = vec![];

        if config.partitioning.column_groups.is_empty() {
            // All columns in group 0
            let all_cols: Vec<String> = config.columns.iter()
                .map(|c| c.name.clone())
                .collect();
            for col in &all_cols {
                column_to_group.insert(col.clone(), 0);
            }
            group_to_columns.push(all_cols);
        } else {
            // Process explicitly defined column groups
            for (idx, group) in config.partitioning.column_groups.iter().enumerate() {
                for col in group {
                    column_to_group.insert(col.clone(), idx as u16);
                }
                group_to_columns.push(group.clone());
            }

            // Collect columns not assigned to any group
            let unassigned: Vec<String> = config.columns.iter()
                .map(|c| c.name.clone())
                .filter(|col| !column_to_group.contains_key(col))
                .collect();

            // If there are unassigned columns, add them to a new group
            if !unassigned.is_empty() {
                let new_group_idx = group_to_columns.len() as u16;
                for col in &unassigned {
                    column_to_group.insert(col.clone(), new_group_idx);
                }
                group_to_columns.push(unassigned);
            }
        }

        Self {
            column_to_group,
            group_to_columns: group_to_columns.clone(),
            num_groups: group_to_columns.len() as u16,
        }
    }

    pub fn get_group(&self, column: &str) -> Option<u16> {
        self.column_to_group.get(column).copied()
    }

    pub fn get_columns_in_group(&self, group: u16) -> &[String] {
        &self.group_to_columns[group as usize]
    }

    pub fn num_groups(&self) -> u16 {
        self.num_groups
    }

    /// Get groups needed for a projection
    pub fn required_groups(&self, columns: &[String]) -> Vec<u16> {
        let mut groups: Vec<u16> = columns.iter()
            .filter_map(|c| self.get_group(c))
            .collect();
        groups.sort();
        groups.dedup();
        groups
    }
}


