use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct OutputSchema {
    pub name: String,
    pub fields: Vec<FieldDef>,
    pub markers: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name: String,
    pub required: bool,
    pub field_type: FieldType,
    pub desc: Option<String>,
    pub examples: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Enum(Vec<String>),
    Array { items: ArrayItems },
    Object { fields: Vec<FieldDef> },
}

#[derive(Debug, Clone)]
pub enum ArrayItems {
    Scalar(Box<FieldType>),
    Object(Vec<FieldDef>),
    Untyped,
}
