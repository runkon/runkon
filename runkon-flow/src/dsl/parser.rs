use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::lexer::{Lexer, Token};
use super::types::{
    AgentRef, AlwaysNode, CallNode, CallWorkflowNode, Condition, DoNode, DoWhileNode, ForEachNode,
    ForeachScope, GateNode, GateOptions, GateType, IfNode, InputDecl, InputType, OnChildFail,
    OnCycle, OnFail, OnFailAction, OnMaxIter, OnTimeout, ParallelNode, QualityGateConfig,
    ScriptNode, TicketScope, UnlessNode, WhileNode, WorkflowDef, WorkflowNode, WorkflowTrigger,
    WorktreeScope,
};

// ---------------------------------------------------------------------------
// Parser helpers
// ---------------------------------------------------------------------------

/// A value from a key-value pair in the DSL, remembering whether it was quoted.
#[derive(Debug, Clone)]
enum KvValue {
    Quoted(String),
    Bare(String),
    Array(Vec<String>),
    Map(HashMap<String, String>),
}

impl KvValue {
    fn as_str(&self) -> &str {
        match self {
            Self::Quoted(s) | Self::Bare(s) => s.as_str(),
            Self::Array(_) => unreachable!("BUG: as_str() called on KvValue::Array"),
            Self::Map(_) => unreachable!("BUG: as_str() called on KvValue::Map"),
        }
    }

    fn into_string(self) -> String {
        match self {
            Self::Quoted(s) | Self::Bare(s) => s,
            Self::Array(_) => unreachable!("BUG: into_string() called on KvValue::Array"),
            Self::Map(_) => unreachable!("BUG: into_string() called on KvValue::Map"),
        }
    }

    fn into_string_array(self) -> Vec<String> {
        match self {
            Self::Array(v) => v,
            Self::Quoted(s) | Self::Bare(s) => vec![s],
            Self::Map(_) => unreachable!("BUG: into_string_array() called on KvValue::Map"),
        }
    }

    fn into_agent_ref(self) -> AgentRef {
        match self {
            Self::Bare(s) => AgentRef::Name(s),
            Self::Quoted(s) if s.contains('/') => AgentRef::Path(s),
            Self::Quoted(s) => AgentRef::Name(s),
            Self::Array(_) => unreachable!("BUG: into_agent_ref() called on KvValue::Array"),
            Self::Map(_) => unreachable!("BUG: into_agent_ref() called on KvValue::Map"),
        }
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    warnings: Vec<String>,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            pos: 0,
            warnings: Vec::new(),
        }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> std::result::Result<(), String> {
        let tok = self.advance();
        if &tok == expected {
            Ok(())
        } else {
            Err(format!("Expected {expected:?}, got {tok:?}"))
        }
    }

    fn expect_ident(&mut self) -> std::result::Result<String, String> {
        match self.advance() {
            Token::Ident(s) => Ok(s),
            Token::Required => Ok("required".to_string()),
            Token::Default => Ok("default".to_string()),
            Token::Description => Ok("description".to_string()),
            Token::Boolean => Ok("boolean".to_string()),
            Token::If => Ok("if".to_string()),
            Token::Workflow => Ok("workflow".to_string()),
            Token::Inputs => Ok("inputs".to_string()),
            other => Err(format!("Expected identifier, got {other:?}")),
        }
    }

    fn expect_value(&mut self) -> std::result::Result<KvValue, String> {
        match self.advance() {
            Token::StringLit(s) => Ok(KvValue::Quoted(s)),
            Token::Int(n) => Ok(KvValue::Bare(n.to_string())),
            Token::Ident(s) => {
                if self.peek() == &Token::Dot {
                    self.advance();
                    let field = self.expect_ident()?;
                    Ok(KvValue::Bare(format!("{s}.{field}")))
                } else {
                    Ok(KvValue::Bare(s))
                }
            }
            Token::Required => Ok(KvValue::Bare("required".to_string())),
            Token::Default => Ok(KvValue::Bare("default".to_string())),
            Token::Description => Ok(KvValue::Bare("description".to_string())),
            Token::Boolean => Ok(KvValue::Bare("boolean".to_string())),
            Token::Call => Ok(KvValue::Bare("call".to_string())),
            Token::If => Ok(KvValue::Bare("if".to_string())),
            Token::Unless => Ok(KvValue::Bare("unless".to_string())),
            Token::While => Ok(KvValue::Bare("while".to_string())),
            Token::Parallel => Ok(KvValue::Bare("parallel".to_string())),
            Token::Gate => Ok(KvValue::Bare("gate".to_string())),
            Token::Always => Ok(KvValue::Bare("always".to_string())),
            Token::Script => Ok(KvValue::Bare("script".to_string())),
            Token::ForEach => Ok(KvValue::Bare("foreach".to_string())),
            Token::LBrace => {
                let kvs = self.parse_kvs()?;
                self.expect(&Token::RBrace)?;
                let map: HashMap<String, String> =
                    kvs.into_iter().map(|(k, v)| (k, v.into_string())).collect();
                Ok(KvValue::Map(map))
            }
            Token::LBracket => {
                let mut items = Vec::new();
                while self.peek() != &Token::RBracket && self.peek() != &Token::Eof {
                    let item = self.expect_value()?;
                    items.push(item.into_string());
                    if self.peek() == &Token::Comma {
                        self.advance();
                    }
                }
                self.expect(&Token::RBracket)?;
                Ok(KvValue::Array(items))
            }
            other => Err(format!(
                "Expected value (string, int, ident, or array), got {other:?}"
            )),
        }
    }

    fn expect_agent_ref(&mut self) -> std::result::Result<AgentRef, String> {
        match self.advance() {
            Token::Ident(s) => Ok(AgentRef::Name(s)),
            Token::Required => Ok(AgentRef::Name("required".to_string())),
            Token::Default => Ok(AgentRef::Name("default".to_string())),
            Token::Description => Ok(AgentRef::Name("description".to_string())),
            Token::StringLit(s) => Ok(AgentRef::Path(s)),
            other => Err(format!(
                "Expected agent name (identifier) or path (quoted string), got {other:?}"
            )),
        }
    }

    fn parse_kvs(&mut self) -> std::result::Result<HashMap<String, KvValue>, String> {
        let mut kvs = HashMap::new();
        loop {
            if self.pos + 1 < self.tokens.len() {
                let is_kv_key = matches!(
                    self.peek(),
                    Token::Ident(_)
                        | Token::Required
                        | Token::Default
                        | Token::Description
                        | Token::If
                        | Token::Workflow
                        | Token::Inputs
                );
                let next_is_eq = self.tokens.get(self.pos + 1) == Some(&Token::Equals);
                if is_kv_key && next_is_eq {
                    let key = self.expect_ident()?;
                    self.expect(&Token::Equals)?;
                    let value = self.expect_value()?;
                    kvs.insert(key, value);
                    continue;
                }
            }
            break;
        }
        Ok(kvs)
    }

    fn parse_workflow(&mut self) -> std::result::Result<WorkflowDef, String> {
        self.expect(&Token::Workflow)?;
        let name = self.expect_ident()?;
        self.expect(&Token::LBrace)?;

        let mut title: Option<String> = None;
        let mut description = String::new();
        let mut trigger = WorkflowTrigger::Manual;
        let mut targets: Vec<String> = Vec::new();
        let mut group: Option<String> = None;
        let mut inputs = Vec::new();
        let mut body = Vec::new();
        let mut always = Vec::new();

        while self.peek() != &Token::RBrace && self.peek() != &Token::Eof {
            match self.peek() {
                Token::Meta => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    let kvs = self.parse_kvs()?;
                    self.expect(&Token::RBrace)?;

                    if let Some(t) = kvs.get("title") {
                        title = Some(t.as_str().to_string());
                    }
                    if let Some(desc) = kvs.get("description") {
                        description = desc.as_str().to_string();
                    }
                    if let Some(trig) = kvs.get("trigger") {
                        let trig_str = trig.as_str();
                        trigger = trig_str
                            .parse::<WorkflowTrigger>()
                            .map_err(|e| format!("In meta block: {e}"))?;
                        if trigger != WorkflowTrigger::Manual {
                            self.warnings.push(format!(
                                "trigger '{}' is not implemented in v1; only 'manual' is active",
                                trig_str
                            ));
                        }
                    }
                    if let Some(tgts) = kvs.get("targets") {
                        targets = tgts.clone().into_string_array();
                    }
                    if let Some(grp) = kvs.get("group") {
                        group = Some(grp.as_str().to_string());
                    }
                }
                Token::Inputs => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    while self.peek() != &Token::RBrace && self.peek() != &Token::Eof {
                        let input_name = self.expect_ident()?;
                        let mut required = false;
                        let mut default: Option<String> = None;
                        let mut description: Option<String> = None;
                        let mut input_type = InputType::String;
                        loop {
                            match self.peek() {
                                Token::Required => {
                                    self.advance();
                                    required = true;
                                }
                                Token::Boolean => {
                                    self.advance();
                                    input_type = InputType::Boolean;
                                }
                                Token::Default => {
                                    self.advance();
                                    self.expect(&Token::Equals)?;
                                    default = Some(self.expect_value()?.into_string());
                                }
                                Token::Description => {
                                    self.advance();
                                    self.expect(&Token::Equals)?;
                                    description = Some(self.expect_value()?.into_string());
                                }
                                _ => break,
                            }
                        }
                        if input_type == InputType::Boolean {
                            required = false;
                        } else if !required && default.is_none() {
                            required = true;
                        }
                        inputs.push(InputDecl {
                            name: input_name,
                            required,
                            default,
                            description,
                            input_type,
                        });
                    }
                    self.expect(&Token::RBrace)?;
                }
                Token::Always => {
                    self.advance();
                    self.expect(&Token::LBrace)?;
                    always.extend(self.parse_body()?);
                }
                _ => {
                    body.push(self.parse_node()?);
                }
            }
        }

        self.expect(&Token::RBrace)?;

        Ok(WorkflowDef {
            name,
            title,
            description,
            trigger,
            targets,
            group,
            inputs,
            body,
            always,
            source_path: String::new(),
        })
    }

    fn parse_node(&mut self) -> std::result::Result<WorkflowNode, String> {
        match self.peek() {
            Token::Call => {
                if self.tokens.get(self.pos + 1) == Some(&Token::Workflow) {
                    self.parse_call_workflow().map(WorkflowNode::CallWorkflow)
                } else {
                    self.parse_call().map(WorkflowNode::Call)
                }
            }
            Token::If => self.parse_if().map(WorkflowNode::If),
            Token::Unless => self.parse_unless().map(WorkflowNode::Unless),
            Token::While => self.parse_while().map(WorkflowNode::While),
            Token::Do => self.parse_do(),
            Token::Parallel => self.parse_parallel().map(WorkflowNode::Parallel),
            Token::Gate => self.parse_gate().map(WorkflowNode::Gate),
            Token::Always => self.parse_always().map(WorkflowNode::Always),
            Token::Script => self.parse_script().map(WorkflowNode::Script),
            Token::ForEach => self.parse_foreach().map(WorkflowNode::ForEach),
            other => Err(format!(
                "Expected a workflow node (call, if, unless, while, do, parallel, gate, always, script, foreach), got {other:?}"
            )),
        }
    }

    fn extract_retries_on_fail_bot_name(
        kvs: &mut HashMap<String, KvValue>,
        err_prefix: &str,
    ) -> std::result::Result<(u32, Option<OnFail>, Option<String>), String> {
        let retries = kvs
            .get("retries")
            .map(|v| v.as_str().parse::<u32>())
            .transpose()
            .map_err(|e| format!("{err_prefix}invalid retries: {e}"))?
            .unwrap_or(0);
        let on_fail = kvs.remove("on_fail").map(|v| {
            if v.as_str() == "continue" {
                OnFail::Continue
            } else {
                OnFail::Agent(v.into_agent_ref())
            }
        });
        let bot_name = kvs.remove("as").map(|v| v.into_string());
        Ok((retries, on_fail, bot_name))
    }

    fn parse_call(&mut self) -> std::result::Result<CallNode, String> {
        self.expect(&Token::Call)?;
        let agent = self.expect_agent_ref()?;

        let mut retries = 0u32;
        let mut on_fail = None;
        let mut output = None;
        let mut with = Vec::new();
        let mut bot_name = None;
        let mut plugin_dirs = Vec::new();
        let mut timeout = None;

        if self.peek() == &Token::LBrace {
            self.advance();
            let mut kvs = self.parse_kvs()?;
            self.expect(&Token::RBrace)?;

            (retries, on_fail, bot_name) = Self::extract_retries_on_fail_bot_name(&mut kvs, "")?;
            if let Some(o) = kvs.remove("output") {
                output = Some(o.into_string());
            }
            if let Some(w) = kvs.remove("with") {
                with = w.into_string_array();
            }
            if let Some(pd) = kvs.remove("plugin_dirs") {
                plugin_dirs = pd.into_string_array();
            }
            if let Some(t) = kvs.remove("timeout") {
                timeout = Some(t.into_string());
            }
        }

        Ok(CallNode {
            agent,
            retries,
            on_fail,
            output,
            with,
            bot_name,
            plugin_dirs,
            timeout,
        })
    }

    fn parse_call_workflow(&mut self) -> std::result::Result<CallWorkflowNode, String> {
        self.expect(&Token::Call)?;
        self.expect(&Token::Workflow)?;
        let workflow_name = self.expect_ident()?;

        let mut inputs = HashMap::new();
        let mut retries = 0u32;
        let mut on_fail = None;
        let mut bot_name = None;

        if self.peek() == &Token::LBrace {
            self.advance();

            let mut kvs = self.parse_kvs()?;

            if self.peek() == &Token::Inputs {
                self.advance();
                self.expect(&Token::LBrace)?;
                let input_kvs = self.parse_kvs()?;
                self.expect(&Token::RBrace)?;
                for (k, v) in input_kvs {
                    inputs.insert(k, v.into_string());
                }
            }

            kvs.extend(self.parse_kvs()?);
            self.expect(&Token::RBrace)?;

            (retries, on_fail, bot_name) = Self::extract_retries_on_fail_bot_name(&mut kvs, "")?;
        }

        Ok(CallWorkflowNode {
            workflow: workflow_name,
            inputs,
            retries,
            on_fail,
            bot_name,
        })
    }

    fn parse_condition(&mut self) -> std::result::Result<Condition, String> {
        let first = self.expect_ident()?;
        if self.peek() == &Token::Dot {
            self.advance();
            let marker = self.expect_ident()?;
            Ok(Condition::StepMarker {
                step: first,
                marker,
            })
        } else {
            Ok(Condition::BoolInput { input: first })
        }
    }

    fn parse_condition_body(
        &mut self,
    ) -> std::result::Result<(Condition, Vec<WorkflowNode>), String> {
        let condition = self.parse_condition()?;
        self.expect(&Token::LBrace)?;
        let _kvs = self.parse_kvs()?;
        let body = self.parse_body()?;
        Ok((condition, body))
    }

    fn parse_body(&mut self) -> std::result::Result<Vec<WorkflowNode>, String> {
        let mut body = Vec::new();
        while self.peek() != &Token::RBrace && self.peek() != &Token::Eof {
            body.push(self.parse_node()?);
        }
        self.expect(&Token::RBrace)?;
        Ok(body)
    }

    fn parse_if(&mut self) -> std::result::Result<IfNode, String> {
        self.expect(&Token::If)?;
        let (condition, body) = self.parse_condition_body()?;
        Ok(IfNode { condition, body })
    }

    fn parse_unless(&mut self) -> std::result::Result<UnlessNode, String> {
        self.expect(&Token::Unless)?;
        let (condition, body) = self.parse_condition_body()?;
        Ok(UnlessNode { condition, body })
    }

    fn parse_loop_options(
        kvs: &HashMap<String, KvValue>,
        loop_kind: &str,
    ) -> std::result::Result<(u32, Option<u32>, OnMaxIter), String> {
        let max_iterations = kvs
            .get("max_iterations")
            .ok_or(format!("{loop_kind} loop requires max_iterations"))?
            .as_str()
            .parse::<u32>()
            .map_err(|e| format!("Invalid max_iterations: {e}"))?;

        let stuck_after = kvs
            .get("stuck_after")
            .map(|v| v.as_str().parse::<u32>())
            .transpose()
            .map_err(|e| format!("Invalid stuck_after: {e}"))?;

        let on_max_iter = match kvs.get("on_max_iter").map(|s| s.as_str()) {
            Some("continue") => OnMaxIter::Continue,
            Some("fail") | None => OnMaxIter::Fail,
            Some(other) => return Err(format!("Invalid on_max_iter: {other}")),
        };

        Ok((max_iterations, stuck_after, on_max_iter))
    }

    fn parse_while(&mut self) -> std::result::Result<WhileNode, String> {
        self.expect(&Token::While)?;
        let (step, marker) = match self.parse_condition()? {
            Condition::StepMarker { step, marker } => (step, marker),
            Condition::BoolInput { input } => {
                return Err(format!(
                    "while condition must be `step.marker`, not a bare identifier `{input}`"
                ))
            }
        };
        self.expect(&Token::LBrace)?;

        let kvs = self.parse_kvs()?;
        let (max_iterations, stuck_after, on_max_iter) = Self::parse_loop_options(&kvs, "while")?;

        let body = self.parse_body()?;

        Ok(WhileNode {
            step,
            marker,
            max_iterations,
            stuck_after,
            on_max_iter,
            body,
        })
    }

    fn parse_do(&mut self) -> std::result::Result<WorkflowNode, String> {
        self.expect(&Token::Do)?;

        if matches!(self.peek(), Token::Ident(_)) {
            return Err(
                "expected `{` after `do`, found identifier\n  hint: do-while syntax is now `do { ... } while x.y`".to_string()
            );
        }
        self.expect(&Token::LBrace)?;

        let mut kvs = self.parse_kvs()?;

        let body = self.parse_body()?;

        if self.peek() == &Token::While {
            self.advance();
            let (step, marker) = match self.parse_condition()? {
                Condition::StepMarker { step, marker } => (step, marker),
                Condition::BoolInput { input } => {
                    return Err(format!(
                        "do-while condition must be `step.marker`, not a bare identifier `{input}`"
                    ))
                }
            };
            let (max_iterations, stuck_after, on_max_iter) = Self::parse_loop_options(&kvs, "do")?;
            Ok(WorkflowNode::DoWhile(DoWhileNode {
                step,
                marker,
                max_iterations,
                stuck_after,
                on_max_iter,
                body,
            }))
        } else {
            let output = kvs.remove("output").map(|v| v.as_str().to_string());
            let with = kvs
                .remove("with")
                .map(|v| v.into_string_array())
                .unwrap_or_default();
            if let Some(key) = kvs.keys().next() {
                return Err(format!(
                    "unknown option `{key}` in plain `do` block (only `output` and `with` are allowed)"
                ));
            }
            Ok(WorkflowNode::Do(DoNode { output, with, body }))
        }
    }

    fn parse_parallel(&mut self) -> std::result::Result<ParallelNode, String> {
        self.expect(&Token::Parallel)?;
        self.expect(&Token::LBrace)?;

        let mut kvs = self.parse_kvs()?;

        let fail_fast = kvs
            .get("fail_fast")
            .map(|v| v.as_str() == "true")
            .unwrap_or(true);

        let min_success = kvs
            .get("min_success")
            .map(|v| v.as_str().parse::<u32>())
            .transpose()
            .map_err(|e| format!("Invalid min_success: {e}"))?;

        let output = kvs.get("output").map(|v| v.as_str().to_string());

        let block_with = kvs
            .remove("with")
            .map(|v| v.into_string_array())
            .unwrap_or_default();

        let mut calls = Vec::new();
        let mut call_outputs: HashMap<String, String> = HashMap::new();
        let mut call_with: HashMap<String, Vec<String>> = HashMap::new();
        let mut call_if: HashMap<String, (String, String)> = HashMap::new();
        while self.peek() == &Token::Call {
            self.advance();
            let agent = self.expect_agent_ref()?;
            let idx = calls.len().to_string();
            if self.peek() == &Token::LBrace {
                self.advance();
                let mut call_kvs = self.parse_kvs()?;
                self.expect(&Token::RBrace)?;
                if let Some(o) = call_kvs.get("output") {
                    call_outputs.insert(idx.clone(), o.as_str().to_string());
                }
                if let Some(w) = call_kvs.remove("with") {
                    call_with.insert(idx.clone(), w.into_string_array());
                }
                if let Some(v) = call_kvs.get("if") {
                    let s = v.as_str();
                    let (step_name, marker_name) = s.split_once('.').ok_or_else(|| {
                        format!("if value `{s}` must be in the form `step.marker` (no dot found)")
                    })?;
                    call_if.insert(idx, (step_name.to_string(), marker_name.to_string()));
                }
            }
            calls.push(agent);
        }
        self.expect(&Token::RBrace)?;

        if calls.is_empty() {
            return Err("parallel block must contain at least one call".to_string());
        }

        Ok(ParallelNode {
            fail_fast,
            min_success,
            calls,
            output,
            call_outputs,
            with: block_with,
            call_with,
            call_if,
        })
    }

    fn parse_gate(&mut self) -> std::result::Result<GateNode, String> {
        self.expect(&Token::Gate)?;
        let name = self.expect_ident()?;

        let gate_type = match name.as_str() {
            "human_approval" => GateType::HumanApproval,
            "human_review" => GateType::HumanReview,
            "pr_approval" => GateType::PrApproval,
            "pr_checks" => GateType::PrChecks,
            "quality_gate" => GateType::QualityGate,
            _ => return Err(format!(
                "Unknown gate type: '{}'. Expected one of: human_approval, human_review, pr_approval, pr_checks, quality_gate",
                name
            )),
        };

        self.expect(&Token::LBrace)?;
        let kvs = self.parse_kvs()?;
        self.expect(&Token::RBrace)?;

        if gate_type == GateType::QualityGate {
            let source = kvs
                .get("source")
                .ok_or("quality_gate requires a `source` field referencing a prior step")?
                .as_str()
                .to_string();
            let threshold = kvs
                .get("threshold")
                .ok_or("quality_gate requires a `threshold` field (0-100)")?
                .as_str()
                .parse::<u32>()
                .map_err(|e| format!("Invalid threshold: {e}"))?;
            if threshold > 100 {
                return Err(format!(
                    "quality_gate threshold must be 0-100, got {threshold}"
                ));
            }
            let on_fail_action = match kvs.get("on_fail").map(|v| v.as_str()) {
                Some("fail") | None => OnFailAction::Fail,
                Some("continue") => OnFailAction::Continue,
                Some(other) => return Err(format!("Invalid on_fail for quality_gate: {other}")),
            };
            let bot_name = kvs.get("as").map(|v| v.as_str().to_string());

            return Ok(GateNode {
                name,
                gate_type,
                prompt: None,
                min_approvals: 1,
                approval_mode: Default::default(),
                timeout_secs: 0,
                on_timeout: OnTimeout::Fail,
                bot_name,
                quality_gate: Some(QualityGateConfig {
                    source,
                    threshold,
                    on_fail_action,
                }),
                options: None,
            });
        }

        let prompt = kvs.get("prompt").map(|v| v.as_str().to_string());
        let min_approvals = kvs
            .get("min_approvals")
            .map(|v| v.as_str().parse::<u32>())
            .transpose()
            .map_err(|e| format!("Invalid min_approvals: {e}"))?
            .unwrap_or(1);

        let approval_mode = match kvs.get("mode").map(|v| v.as_str()) {
            Some("review_decision") => super::types::ApprovalMode::ReviewDecision,
            Some("min_approvals") | None => super::types::ApprovalMode::MinApprovals,
            Some(other) => return Err(format!("Invalid mode for pr_approval: {other}")),
        };
        if approval_mode == super::types::ApprovalMode::ReviewDecision
            && kvs.contains_key("min_approvals")
        {
            return Err(
                "Cannot specify both mode = \"review_decision\" and min_approvals".to_string(),
            );
        }

        let timeout_secs = kvs
            .get("timeout")
            .map(|v| parse_duration_str(v.as_str()))
            .transpose()?
            .unwrap_or(24 * 3600);

        let on_timeout = match kvs.get("on_timeout").map(|s| s.as_str()) {
            Some("continue") => OnTimeout::Continue,
            Some("fail") | None => OnTimeout::Fail,
            Some(other) => return Err(format!("Invalid on_timeout: {other}")),
        };

        let bot_name = kvs.get("as").map(|v| v.as_str().to_string());

        let options = match kvs.get("options") {
            None => None,
            Some(v) => {
                match gate_type {
                    GateType::HumanApproval | GateType::HumanReview => {}
                    _ => {
                        return Err(format!(
                            "`options` is only valid on human_approval / human_review gates, not '{gate_type}'"
                        ));
                    }
                }
                let parsed = match v {
                    KvValue::Array(items) => GateOptions::Static(items.clone()),
                    KvValue::Bare(s) | KvValue::Quoted(s) if s.contains('.') => {
                        GateOptions::StepRef(s.clone())
                    }
                    KvValue::Bare(s) | KvValue::Quoted(s) => {
                        return Err(format!(
                            "Invalid `options` value '{s}': expected an array [\"...\"] or a step field reference like 'step.field'"
                        ));
                    }
                    KvValue::Map(_) => {
                        return Err(
                            "`options` must be an array or step field reference, not a map"
                                .to_string(),
                        );
                    }
                };
                Some(parsed)
            }
        };

        Ok(GateNode {
            name,
            gate_type,
            prompt,
            min_approvals,
            approval_mode,
            timeout_secs,
            on_timeout,
            bot_name,
            quality_gate: None,
            options,
        })
    }

    fn parse_always(&mut self) -> std::result::Result<AlwaysNode, String> {
        self.expect(&Token::Always)?;
        self.expect(&Token::LBrace)?;
        let body = self.parse_body()?;
        Ok(AlwaysNode { body })
    }

    fn parse_script(&mut self) -> std::result::Result<ScriptNode, String> {
        self.expect(&Token::Script)?;
        let name = self.expect_ident()?;
        self.expect(&Token::LBrace)?;

        let mut kvs = self.parse_kvs()?;
        self.expect(&Token::RBrace)?;

        let run = kvs
            .remove("run")
            .ok_or_else(|| format!("script '{name}' is missing required `run` field"))?
            .into_string();

        let env = kvs
            .remove("env")
            .map(|v| match v {
                KvValue::Map(m) => Ok(m),
                _ => Err(format!(
                    "script '{name}': `env` must be a map `{{ KEY = \"value\" }}`"
                )),
            })
            .transpose()?
            .unwrap_or_default();

        let timeout = kvs
            .get("timeout")
            .map(|v| v.as_str().parse::<u64>())
            .transpose()
            .map_err(|e| format!("script '{name}': invalid timeout: {e}"))?;

        let (retries, on_fail, bot_name) =
            Self::extract_retries_on_fail_bot_name(&mut kvs, &format!("script '{name}': "))?;

        Ok(ScriptNode {
            name,
            run,
            env,
            timeout,
            retries,
            on_fail,
            bot_name,
        })
    }

    fn parse_foreach(&mut self) -> std::result::Result<ForEachNode, String> {
        self.expect(&Token::ForEach)?;
        let name = self.expect_ident()?;
        self.expect(&Token::LBrace)?;

        let mut kvs = self.parse_kvs()?;
        self.expect(&Token::RBrace)?;

        let over: String = match kvs.get("over").map(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return Err(format!("foreach '{name}': missing required key 'over'")),
        };

        let max_parallel = kvs
            .get("max_parallel")
            .ok_or_else(|| format!("foreach '{name}': missing required key 'max_parallel'"))?
            .as_str()
            .parse::<u32>()
            .map_err(|e| format!("foreach '{name}': invalid max_parallel: {e}"))?;

        let workflow = kvs
            .get("workflow")
            .ok_or_else(|| format!("foreach '{name}': missing required key 'workflow'"))?
            .as_str()
            .to_string();

        let scope = if let Some(s) = kvs.remove("scope") {
            match s {
                KvValue::Map(m) => match over.as_str() {
                    "worktrees" => {
                        let base_branch = m.get("base_branch").cloned();
                        let has_open_pr = match m.get("has_open_pr").map(|s| s.as_str()) {
                            None => None,
                            Some("true") => Some(true),
                            Some("false") => Some(false),
                            Some(v) => {
                                return Err(format!(
                                    "foreach '{name}': scope.has_open_pr must be true or false, got '{v}'"
                                ))
                            }
                        };
                        Some(ForeachScope::Worktree(WorktreeScope {
                            base_branch,
                            has_open_pr,
                        }))
                    }
                    "tickets" => {
                        if let Some(ticket_id) = m.get("ticket_id") {
                            Some(ForeachScope::Ticket(TicketScope::TicketId(
                                ticket_id.clone(),
                            )))
                        } else if let Some(label) = m.get("label") {
                            Some(ForeachScope::Ticket(TicketScope::Label(label.clone())))
                        } else if let Some(v) = m.get("unlabeled") {
                            if v == "true" {
                                Some(ForeachScope::Ticket(TicketScope::Unlabeled))
                            } else {
                                return Err(format!(
                                    "foreach '{name}': scope.unlabeled must be true"
                                ));
                            }
                        } else {
                            return Err(format!(
                                "foreach '{name}': scope must contain ticket_id, label, or unlabeled"
                            ));
                        }
                    }
                    _ => {
                        return Err(format!(
                            "foreach '{name}': scope is not applicable for over = '{over}'"
                        ))
                    }
                },
                _ => return Err(format!("foreach '{name}': scope must be a map")),
            }
        } else {
            None
        };

        let filter = if let Some(f) = kvs.remove("filter") {
            match f {
                KvValue::Map(m) => m,
                _ => {
                    return Err(format!(
                        "foreach '{name}': filter must be a map {{ key = \"value\" }}"
                    ))
                }
            }
        } else {
            HashMap::new()
        };

        let inputs = if let Some(inp) = kvs.remove("inputs") {
            match inp {
                KvValue::Map(m) => m,
                _ => {
                    return Err(format!(
                        "foreach '{name}': inputs must be a map {{ key = \"value\" }}"
                    ))
                }
            }
        } else {
            HashMap::new()
        };

        let ordered = match kvs.get("ordered").map(|v| v.as_str()) {
            Some("true") => true,
            Some("false") | None => false,
            Some(other) => {
                return Err(format!(
                    "foreach '{name}': invalid ordered value '{other}' (expected: true or false)"
                ))
            }
        };

        let on_cycle = match kvs.get("on_cycle").map(|v| v.as_str()) {
            Some("warn") => OnCycle::Warn,
            Some("fail") | None => OnCycle::Fail,
            Some(other) => {
                return Err(format!(
                    "foreach '{name}': invalid on_cycle value '{other}' (expected: fail, warn)"
                ))
            }
        };

        let on_child_fail = match kvs.get("on_child_fail").map(|v| v.as_str()) {
            Some("halt") => OnChildFail::Halt,
            Some("continue") => OnChildFail::Continue,
            Some("skip_dependents") => OnChildFail::SkipDependents,
            None => OnChildFail::Continue,
            Some(other) => {
                return Err(format!(
                    "foreach '{name}': invalid on_child_fail value '{other}' \
                     (expected: halt, continue, skip_dependents)"
                ))
            }
        };

        Ok(ForEachNode {
            name,
            over,
            scope,
            filter,
            ordered,
            on_cycle,
            max_parallel,
            workflow,
            inputs,
            on_child_fail,
        })
    }
}

// ---------------------------------------------------------------------------
// Duration parser
// ---------------------------------------------------------------------------

pub(crate) fn parse_duration_str(s: &str) -> std::result::Result<u64, String> {
    let s = s.trim().trim_matches('"');
    if let Some(hours) = s.strip_suffix('h') {
        let n: u64 = hours
            .parse()
            .map_err(|e| format!("Invalid duration '{s}': {e}"))?;
        n.checked_mul(3600)
            .ok_or_else(|| format!("Duration overflow: '{s}' exceeds u64 range"))
    } else if let Some(mins) = s.strip_suffix('m') {
        let n: u64 = mins
            .parse()
            .map_err(|e| format!("Invalid duration '{s}': {e}"))?;
        n.checked_mul(60)
            .ok_or_else(|| format!("Duration overflow: '{s}' exceeds u64 range"))
    } else if let Some(secs) = s.strip_suffix('s') {
        secs.parse()
            .map_err(|e| format!("Invalid duration '{s}': {e}"))
    } else {
        s.parse()
            .map_err(|e| format!("Invalid duration '{s}': {e}"))
    }
}

// ---------------------------------------------------------------------------
// Public parse entry points
// ---------------------------------------------------------------------------

pub(crate) fn parse_workflow_file(path: &Path) -> Result<WorkflowDef, String> {
    let content =
        fs::read_to_string(path).map_err(|e| format!("Failed to read {}: {e}", path.display()))?;

    parse_workflow_str(&content, path.to_string_lossy().as_ref())
}

pub fn parse_workflow_str(input: &str, source_path: &str) -> Result<WorkflowDef, String> {
    let mut lexer = Lexer::new(input);
    let mut tokens = Vec::new();
    loop {
        let tok = lexer
            .next_token()
            .map_err(|e| format!("Lexer error in {source_path}: {e}"))?;
        let is_eof = tok == Token::Eof;
        tokens.push(tok);
        if is_eof {
            break;
        }
    }

    let mut parser = Parser::new(tokens);
    let mut def = parser
        .parse_workflow()
        .map_err(|e| format!("Parse error in {source_path}: {e}"))?;
    def.source_path = source_path.to_string();

    for warning in &parser.warnings {
        tracing::warn!("Warning in {source_path}: {warning}");
    }

    Ok(def)
}

#[cfg(test)]
mod tests {
    use super::parse_workflow_str;
    use crate::dsl::{AgentRef, Condition, GateType, InputType, WorkflowNode, WorkflowTrigger};

    // ---- basic workflow structure ----

    #[test]
    fn parse_minimal_workflow() {
        let src = "workflow my_wf {\n}";
        let def = parse_workflow_str(src, "test.wf").unwrap();
        assert_eq!(def.name, "my_wf");
        assert!(def.body.is_empty());
        assert!(def.always.is_empty());
        assert_eq!(def.source_path, "test.wf");
    }

    #[test]
    fn parse_workflow_with_description_and_trigger() {
        let src = r#"
workflow deploy {
    meta {
        description = "Deploy the service"
        trigger = manual
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        assert_eq!(def.name, "deploy");
        assert_eq!(def.description, "Deploy the service");
        assert_eq!(def.trigger, WorkflowTrigger::Manual);
    }

    #[test]
    fn parse_single_call_node() {
        let src = r#"
workflow wf {
    call plan
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        assert_eq!(def.body.len(), 1);
        match &def.body[0] {
            WorkflowNode::Call(c) => assert_eq!(c.agent, AgentRef::Name("plan".to_string())),
            other => panic!("expected Call node, got {other:?}"),
        }
    }

    #[test]
    fn parse_call_node_with_path_agent() {
        let src = r#"
workflow wf {
    call ".claude/agents/plan.md"
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        match &def.body[0] {
            WorkflowNode::Call(c) => {
                assert_eq!(
                    c.agent,
                    AgentRef::Path(".claude/agents/plan.md".to_string())
                );
                assert_eq!(c.agent.step_key(), "plan");
            }
            other => panic!("expected Call node, got {other:?}"),
        }
    }

    #[test]
    fn parse_script_node() {
        let src = r#"
workflow wf {
    script lint {
        run = "./scripts/lint.sh"
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        match &def.body[0] {
            WorkflowNode::Script(s) => {
                assert_eq!(s.name, "lint");
                assert_eq!(s.run, "./scripts/lint.sh");
            }
            other => panic!("expected Script node, got {other:?}"),
        }
    }

    #[test]
    fn parse_if_with_step_marker_condition() {
        let src = r#"
workflow wf {
    call step1
    if step1.approved {
        call step2
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        assert_eq!(def.body.len(), 2);
        match &def.body[1] {
            WorkflowNode::If(n) => match &n.condition {
                Condition::StepMarker { step, marker } => {
                    assert_eq!(step, "step1");
                    assert_eq!(marker, "approved");
                }
                other => panic!("expected StepMarker condition, got {other:?}"),
            },
            other => panic!("expected If node, got {other:?}"),
        }
    }

    #[test]
    fn parse_unless_with_bool_input_condition() {
        let src = r#"
workflow wf {
    inputs {
        skip_tests boolean
    }
    unless skip_tests {
        call run_tests
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        match &def.body[0] {
            WorkflowNode::Unless(n) => match &n.condition {
                Condition::BoolInput { input } => assert_eq!(input, "skip_tests"),
                other => panic!("expected BoolInput condition, got {other:?}"),
            },
            other => panic!("expected Unless node, got {other:?}"),
        }
    }

    #[test]
    fn parse_while_node() {
        let src = r#"
workflow wf {
    call review
    while review.needs_revision {
        max_iterations = 5
        call fix
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        match &def.body[1] {
            WorkflowNode::While(w) => {
                assert_eq!(w.step, "review");
                assert_eq!(w.marker, "needs_revision");
                assert_eq!(w.max_iterations, 5);
                assert_eq!(w.body.len(), 1);
            }
            other => panic!("expected While node, got {other:?}"),
        }
    }

    #[test]
    fn parse_gate_human_approval() {
        let src = r#"
workflow wf {
    gate human_approval {
        prompt = "Approve deployment?"
        timeout = 3600
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        match &def.body[0] {
            WorkflowNode::Gate(g) => {
                assert_eq!(g.gate_type, GateType::HumanApproval);
                assert_eq!(g.prompt.as_deref(), Some("Approve deployment?"));
                assert_eq!(g.timeout_secs, 3600);
            }
            other => panic!("expected Gate node, got {other:?}"),
        }
    }

    #[test]
    fn parse_inputs_block() {
        let src = r#"
workflow wf {
    inputs {
        env required
        debug boolean
        branch default = "main"
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        assert_eq!(def.inputs.len(), 3);

        let env_input = def.inputs.iter().find(|i| i.name == "env").unwrap();
        assert!(env_input.required);
        assert_eq!(env_input.input_type, InputType::String);

        let debug_input = def.inputs.iter().find(|i| i.name == "debug").unwrap();
        assert_eq!(debug_input.input_type, InputType::Boolean);
        assert!(!debug_input.required);

        let branch_input = def.inputs.iter().find(|i| i.name == "branch").unwrap();
        assert_eq!(branch_input.default.as_deref(), Some("main"));
    }

    #[test]
    fn parse_always_block() {
        let src = r#"
workflow wf {
    call main_step
    always {
        call cleanup
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        assert_eq!(def.body.len(), 1);
        assert_eq!(def.always.len(), 1);
        match &def.always[0] {
            WorkflowNode::Call(c) => assert_eq!(c.agent.step_key(), "cleanup"),
            other => panic!("expected Call node in always block, got {other:?}"),
        }
    }

    #[test]
    fn parse_do_block() {
        let src = r#"
workflow wf {
    do {
        call step_a
        call step_b
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        match &def.body[0] {
            WorkflowNode::Do(d) => {
                assert_eq!(d.body.len(), 2);
            }
            other => panic!("expected Do node, got {other:?}"),
        }
    }

    #[test]
    fn parse_call_workflow_node() {
        let src = r#"
workflow wf {
    call workflow child_wf {
        inputs {
            env = "production"
        }
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        match &def.body[0] {
            WorkflowNode::CallWorkflow(cw) => {
                assert_eq!(cw.workflow, "child_wf");
                assert_eq!(cw.inputs.get("env").map(|s| s.as_str()), Some("production"));
            }
            other => panic!("expected CallWorkflow node, got {other:?}"),
        }
    }

    #[test]
    fn parse_error_on_invalid_syntax() {
        let src = "this is not valid DSL";
        let result = parse_workflow_str(src, "bad.wf");
        assert!(result.is_err(), "invalid syntax should return an error");
    }

    #[test]
    fn parse_error_missing_workflow_keyword() {
        let src = "my_wf { call plan }";
        let result = parse_workflow_str(src, "bad.wf");
        assert!(result.is_err());
    }

    #[test]
    fn parse_parallel_node() {
        let src = r#"
workflow wf {
    parallel {
        call agent_a
        call agent_b
    }
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        match &def.body[0] {
            WorkflowNode::Parallel(p) => {
                assert_eq!(p.calls.len(), 2);
                assert_eq!(p.calls[0], AgentRef::Name("agent_a".to_string()));
                assert_eq!(p.calls[1], AgentRef::Name("agent_b".to_string()));
            }
            other => panic!("expected Parallel node, got {other:?}"),
        }
    }

    #[test]
    fn parse_meta_targets() {
        let src = r#"
workflow wf {
    meta {
        targets = ["worktree", "ticket"]
    }
    call agent
}
"#;
        let def = parse_workflow_str(src, "t.wf").unwrap();
        assert_eq!(def.targets, vec!["worktree", "ticket"]);
    }

    #[test]
    fn parse_source_path_is_set() {
        let src = "workflow wf {}";
        let def = parse_workflow_str(src, "my/path/wf.wf").unwrap();
        assert_eq!(def.source_path, "my/path/wf.wf");
    }
}
