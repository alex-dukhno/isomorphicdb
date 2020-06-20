// Most of the code is taken from
// https://github.com/sfackler/rust-postgres/tree/master/codegen/src

use marksman_escape::Escape;
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::iter;
use std::str;

use crate::snake_to_camel;

const PG_TYPE_DAT: &str = include_str!("pg_type.dat");

struct Type {
    name: String,
    variant: String,
    doc: String,
    len: i16,
}

pub fn build() -> io::Result<()> {
    let mut file = BufWriter::new(File::create("./src/sql_types/src/type_gen.rs")?);
    let types = parse_types();

    make_enum(&mut file, &types)?;
    make_impl(&mut file, &types)
}

struct DatParser<'a> {
    it: iter::Peekable<str::CharIndices<'a>>,
    s: &'a str,
}

impl<'a> DatParser<'a> {
    fn new(s: &'a str) -> DatParser<'a> {
        DatParser {
            it: s.char_indices().peekable(),
            s,
        }
    }

    fn parse_array(&mut self) -> Vec<HashMap<String, String>> {
        self.eat('[');
        let mut vec = vec![];
        while !self.try_eat(']') {
            let object = self.parse_object();
            vec.push(object);
        }
        self.eof();

        vec
    }

    fn parse_object(&mut self) -> HashMap<String, String> {
        let mut object = HashMap::new();

        self.eat('{');
        loop {
            let key = self.parse_ident();
            self.eat('=');
            self.eat('>');
            let value = self.parse_string();
            object.insert(key, value);
            if !self.try_eat(',') {
                break;
            }
        }
        self.eat('}');
        self.eat(',');

        object
    }

    fn parse_ident(&mut self) -> String {
        self.skip_ws();

        let start = match self.it.peek() {
            Some((i, _)) => *i,
            None => return "".to_string(),
        };

        loop {
            match self.it.peek() {
                Some((_, 'a'..='z')) | Some((_, '_')) => {
                    self.it.next();
                }
                Some((i, _)) => return self.s[start..*i].to_string(),
                None => return self.s[start..].to_string(),
            }
        }
    }

    fn parse_string(&mut self) -> String {
        self.skip_ws();

        let mut s = String::new();

        self.eat('\'');
        loop {
            match self.it.next() {
                Some((_, '\'')) => return s,
                Some((_, '\\')) => {
                    let (_, ch) = self.it.next().expect("unexpected eof");
                    s.push(ch);
                }
                Some((_, ch)) => s.push(ch),
                None => panic!("unexpected eof"),
            }
        }
    }

    fn eat(&mut self, target: char) {
        self.skip_ws();

        match self.it.next() {
            Some((_, ch)) if ch == target => {}
            Some((_, ch)) => panic!("expected {} but got {}", target, ch),
            None => panic!("expected {} but got eof", target),
        }
    }

    fn try_eat(&mut self, target: char) -> bool {
        if self.peek(target) {
            self.eat(target);
            true
        } else {
            false
        }
    }

    fn peek(&mut self, target: char) -> bool {
        self.skip_ws();

        match self.it.peek() {
            Some((_, ch)) if *ch == target => true,
            _ => false,
        }
    }

    fn eof(&mut self) {
        self.skip_ws();
        if let Some((_, ch)) = self.it.next() {
            panic!("expected eof but got {}", ch);
        }
    }

    fn skip_ws(&mut self) {
        loop {
            match self.it.peek() {
                Some(&(_, '#')) => self.skip_to('\n'),
                Some(&(_, '\n')) | Some(&(_, ' ')) | Some(&(_, '\t')) => {
                    self.it.next();
                }
                _ => break,
            }
        }
    }

    fn skip_to(&mut self, target: char) {
        for (_, ch) in &mut self.it {
            if ch == target {
                break;
            }
        }
    }
}

fn parse_types() -> BTreeMap<i32, Type> {
    let raw_types = DatParser::new(PG_TYPE_DAT).parse_array();

    let range_vector_re = Regex::new("(range|vector)$").unwrap();
    let array_re = Regex::new("^_(.*)").unwrap();

    let mut types = BTreeMap::new();

    for raw_type in raw_types {
        let oid = raw_type["oid"].parse::<i32>().unwrap();

        let name = raw_type["typname"].clone();

        let ident = range_vector_re.replace(&name, "_$1");
        let ident = array_re.replace(&ident, "${1}_array");
        let variant = snake_to_camel(&ident);

        let len = raw_type["typlen"].parse::<i16>().unwrap_or_default();

        let doc_name = array_re.replace(&name, "$1[]").to_ascii_uppercase();
        let mut doc = doc_name.clone();
        if let Some(descr) = raw_type.get("descr") {
            write!(doc, " - {}", descr).unwrap();
        }
        let doc = Escape::new(doc.as_bytes().iter().cloned()).collect();
        let doc = String::from_utf8(doc).unwrap();

        // Arrays are not yet supported
        // if let Some(array_type_oid) = raw_type.get("array_type_oid") {
        //     let array_type_oid = array_type_oid.parse::<i32>().unwrap();
        //
        //     let name = format!("_{}", name);
        //     let variant = format!("{}Array", variant);
        //     let doc = format!("{}&#91;&#93;", doc_name);
        //     let ident = format!("{}_ARRAY", ident);
        //
        //     let type_ = Type {
        //         name,
        //         variant,
        //         ident,
        //         kind: "A".to_string(),
        //         element: oid,
        //         doc,
        //         len,
        //     };
        //     types.insert(array_type_oid, type_);
        // }

        let type_ = Type {
            name,
            variant,
            doc,
            len,
        };
        types.insert(oid, type_);
    }

    types
}

fn make_enum(w: &mut BufWriter<File>, types: &BTreeMap<i32, Type>) -> io::Result<()> {
    write!(
        w,
        "// Autogenerated file - DO NOT EDIT

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub enum SqlType {{"
    )?;

    for type_ in types.values() {
        write!(
            w,
            r"
    /// {}
    {},",
            type_.doc, type_.variant
        )?;
    }

    write!(
        w,
        r"
}}

"
    )
}

fn make_impl(w: &mut BufWriter<File>, types: &BTreeMap<i32, Type>) -> io::Result<()> {
    write!(
        w,
        "#[allow(clippy::len_without_is_empty)]
impl SqlType {{
    pub fn from_oid(oid: i32) -> Option<SqlType> {{
        match oid {{
",
    )?;

    for (oid, type_) in types {
        writeln!(w, "            {} => Some(SqlType::{}),", oid, type_.variant)?;
    }

    write!(
        w,
        "            _ => None,
        }}
    }}

    pub fn oid(&self) -> i32 {{
        match *self {{
",
    )?;

    for (oid, type_) in types {
        writeln!(w, "            SqlType::{} => {},", type_.variant, oid)?;
    }

    write!(
        w,
        r#"        }}
    }}

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<SqlType> {{
        match s {{
"#,
    )?;

    for type_ in types.values() {
        writeln!(
            w,
            r#"            "{}" => Some(SqlType::{}),"#,
            type_.name, type_.variant
        )?;
    }

    write!(
        w,
        r#"            _ => None,
        }}
    }}

    pub fn name(&self) -> &str {{
        match *self {{
"#,
    )?;

    for type_ in types.values() {
        writeln!(w, r#"            SqlType::{} => "{}","#, type_.variant, type_.name)?;
    }

    write!(
        w,
        r#"        }}
    }}

    pub fn len(&self) -> i16 {{
        match *self {{
"#,
    )?;

    for type_ in types.values() {
        writeln!(w, "            SqlType::{} => {},", type_.variant, type_.len)?;
    }

    write!(
        w,
        "        }}
    }}
}}
"
    )
}
