use std::any::Any;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicBool, Arc};

use runkon_flow::engine_error::EngineError;
use runkon_flow::traits::item_provider::{FanOutItem, ItemProvider, ProviderInfo};
use runkon_flow::traits::run_context::RunContext;

/// `ItemProvider` that returns a fixed list of items for fan-out steps.
struct StaticItemsProvider(Vec<FanOutItem>);

impl ItemProvider for StaticItemsProvider {
    fn name(&self) -> &str {
        "static"
    }

    fn items(
        &self,
        _ctx: &dyn RunContext,
        _info: &ProviderInfo,
        _scope: Option<&dyn Any>,
        _filter: &HashMap<String, String>,
    ) -> Result<Vec<FanOutItem>, EngineError> {
        Ok(self
            .0
            .iter()
            .map(|i| FanOutItem {
                item_type: i.item_type.clone(),
                item_id: i.item_id.clone(),
                item_ref: i.item_ref.clone(),
                context: i.context.clone(),
            })
            .collect())
    }
}

struct StubCtx(PathBuf);

impl RunContext for StubCtx {
    fn injected_variables(&self) -> HashMap<&'static str, String> {
        HashMap::new()
    }
    fn working_dir(&self) -> &Path {
        &self.0
    }
    fn working_dir_str(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }
    fn get(&self, _: &str) -> Option<String> {
        None
    }
    fn run_id(&self) -> &str {
        "static-items-run"
    }
    fn workflow_name(&self) -> &str {
        "static-items-example"
    }
    fn parent_run_id(&self) -> Option<&str> {
        None
    }
    fn shutdown(&self) -> Option<&Arc<AtomicBool>> {
        None
    }
}

fn main() {
    let provider = StaticItemsProvider(vec![
        FanOutItem {
            item_type: "repo".into(),
            item_id: "repo-1".into(),
            item_ref: "main".into(),
            context: HashMap::new(),
        },
        FanOutItem {
            item_type: "repo".into(),
            item_id: "repo-2".into(),
            item_ref: "main".into(),
            context: HashMap::new(),
        },
        FanOutItem {
            item_type: "repo".into(),
            item_id: "repo-3".into(),
            item_ref: "main".into(),
            context: HashMap::new(),
        },
    ]);

    let ctx = StubCtx(std::env::temp_dir());
    let info = ProviderInfo {
        step_id: "foreach-step".into(),
    };
    let items = provider
        .items(&ctx, &info, None, &HashMap::new())
        .expect("items failed");

    println!("provided {} items:", items.len());
    for item in &items {
        println!("  {} / {}", item.item_type, item.item_id);
    }
}
