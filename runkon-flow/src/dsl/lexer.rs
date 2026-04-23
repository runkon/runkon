// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Token {
    // Keywords
    Workflow,
    Meta,
    Inputs,
    Call,
    If,
    Unless,
    While,
    Do,
    Parallel,
    Gate,
    Always,
    Script,
    ForEach,
    Required,
    Default,
    Description,
    Boolean,
    // Punctuation
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Comma,
    Equals,
    Dot,
    // Literals
    Ident(String),
    StringLit(String),
    Int(u32),
    // End
    Eof,
}

pub(super) struct Lexer {
    chars: Vec<char>,
    pos: usize,
    line: usize,
    col: usize,
}

impl Lexer {
    pub(super) fn new(input: &str) -> Self {
        Self {
            chars: input.chars().collect(),
            pos: 0,
            line: 1,
            col: 1,
        }
    }

    fn location(&self) -> String {
        format!("line {}, col {}", self.line, self.col)
    }

    fn peek_char(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.chars.get(self.pos).copied()?;
        self.pos += 1;
        if ch == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(ch)
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while self.peek_char().is_some_and(|c| c.is_whitespace()) {
                self.advance();
            }
            // Skip line comments
            if self.pos + 1 < self.chars.len()
                && self.chars[self.pos] == '/'
                && self.chars[self.pos + 1] == '/'
            {
                while self.peek_char().is_some_and(|c| c != '\n') {
                    self.advance();
                }
                continue;
            }
            break;
        }
    }

    pub(super) fn next_token(&mut self) -> std::result::Result<Token, String> {
        self.skip_whitespace_and_comments();

        let Some(ch) = self.peek_char() else {
            return Ok(Token::Eof);
        };

        match ch {
            '{' => {
                self.advance();
                Ok(Token::LBrace)
            }
            '}' => {
                self.advance();
                Ok(Token::RBrace)
            }
            '[' => {
                self.advance();
                Ok(Token::LBracket)
            }
            ']' => {
                self.advance();
                Ok(Token::RBracket)
            }
            ',' => {
                self.advance();
                Ok(Token::Comma)
            }
            '=' => {
                self.advance();
                Ok(Token::Equals)
            }
            '.' => {
                self.advance();
                Ok(Token::Dot)
            }
            '"' => self.read_string(),
            c if c.is_ascii_digit() => self.read_int(),
            c if c.is_ascii_alphabetic() || c == '_' => self.read_ident_or_keyword(),
            _ => Err(format!(
                "Unexpected character '{}' at {}",
                ch,
                self.location()
            )),
        }
    }

    fn read_string(&mut self) -> std::result::Result<Token, String> {
        self.advance(); // skip opening quote
        let mut value = String::new();
        loop {
            match self.advance() {
                Some('"') => return Ok(Token::StringLit(value)),
                Some('\\') => match self.advance() {
                    Some('n') => value.push('\n'),
                    Some('t') => value.push('\t'),
                    Some('"') => value.push('"'),
                    Some('\\') => value.push('\\'),
                    Some(c) => value.push(c),
                    None => return Err("Unterminated string escape".to_string()),
                },
                Some(c) => value.push(c),
                None => return Err("Unterminated string literal".to_string()),
            }
        }
    }

    fn read_int(&mut self) -> std::result::Result<Token, String> {
        let mut s = String::new();
        while self.peek_char().is_some_and(|c| c.is_ascii_digit()) {
            s.push(self.advance().unwrap());
        }
        s.parse::<u32>()
            .map(Token::Int)
            .map_err(|e| format!("Invalid integer '{}': {e}", s))
    }

    fn read_ident_or_keyword(&mut self) -> std::result::Result<Token, String> {
        let mut s = String::new();
        while self
            .peek_char()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            s.push(self.advance().unwrap());
        }
        Ok(match s.as_str() {
            "workflow" => Token::Workflow,
            "meta" => Token::Meta,
            "inputs" => Token::Inputs,
            "call" => Token::Call,
            "if" => Token::If,
            "unless" => Token::Unless,
            "while" => Token::While,
            "do" => Token::Do,
            "parallel" => Token::Parallel,
            "gate" => Token::Gate,
            "always" => Token::Always,
            "script" => Token::Script,
            "foreach" => Token::ForEach,
            "required" => Token::Required,
            "default" => Token::Default,
            "description" => Token::Description,
            "boolean" => Token::Boolean,
            _ => Token::Ident(s),
        })
    }
}
