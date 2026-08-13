//! Parses idl/controller-clusters-V1.6.0.0.matter into static Rust tables.
//! Focused extractor, not a general IDL parser: clusters, attributes,
//! commands, (request/response/plain) structs, events. Enum/bitmap blocks
//! are skipped. Unrecognized lines are ignored.

use std::fmt::Write as _;
use std::path::PathBuf;

#[derive(Default)]
struct Cluster {
    name: String,
    code: u64,
    attrs: Vec<(u64, String, String, bool)>,      // code, name, ty, is_list
    cmds: Vec<(u64, String, Option<String>, Option<String>, bool)>, // code, name, input, output, timed
    structs: Vec<(String, Vec<(u64, String, String, bool)>)>,
    events: Vec<(u64, String, Vec<(u64, String, String, bool)>)>,
}

fn main() {
    println!("cargo:rerun-if-changed=idl/controller-clusters-V1.6.0.0.matter");
    println!("cargo:rerun-if-changed=build.rs");
    let src = std::fs::read_to_string("idl/controller-clusters-V1.6.0.0.matter").unwrap();
    let clusters = parse(&src);
    let out = render(&clusters);
    let dest = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("tables.rs");
    std::fs::write(dest, out).unwrap();
}

/// "int8u level = 0;" / "optional nullable CharString<32> foo = 1;" /
/// "DeviceTypeStruct deviceTypeList[] = 0;" -> (code, name, ty, is_list)
fn parse_field(line: &str) -> Option<(u64, String, String, bool)> {
    let line = line.trim().trim_end_matches(';');
    let (decl, code) = line.rsplit_once('=')?;
    let code: u64 = code.trim().parse().ok()?;
    let mut toks: Vec<&str> = decl.split_whitespace().collect();
    // strip qualifiers from the front. NB: "fabric_idx" is a *type* (the
    // Fabric Index data type), not a qualifier -- it must not be stripped,
    // or fields like "optional fabric_idx fabricIndex = 1;" lose their type
    // token and get dropped (toks.len() < 2 below).
    while matches!(toks.first().copied(), Some("optional" | "nullable" | "readonly" | "fabric_sensitive")) {
        toks.remove(0);
    }
    if toks.len() < 2 { return None; }
    let name_tok = toks[toks.len() - 1];
    let ty_tok = toks[toks.len() - 2];
    let is_list = name_tok.ends_with("[]");
    let name = name_tok.trim_end_matches("[]").to_string();
    // "octet_string<32>" -> "octet_string"
    let ty = ty_tok.split('<').next().unwrap_or(ty_tok).to_string();
    Some((code, name, ty, is_list))
}

fn parse(src: &str) -> Vec<Cluster> {
    let mut clusters: Vec<Cluster> = Vec::new();
    let mut cur: Option<Cluster> = None;
    // (kind, name/code, fields) for the struct/event block being collected
    enum Block { Skip, Struct(String), Event(u64, String) }
    let mut block: Option<(Block, Vec<(u64, String, String, bool)>)> = None;
    let mut depth_in_block = 0i32;

    for raw in src.lines() {
        let line = raw.trim();
        if line.starts_with("//") || line.starts_with("/*") || line.is_empty() { continue; }

        if let Some((kind, fields)) = block.as_mut() {
            if line.contains('{') { depth_in_block += 1; }
            if line.contains('}') {
                depth_in_block -= 1;
                if depth_in_block <= 0 {
                    let (kind, fields) = block.take().unwrap();
                    if let Some(c) = cur.as_mut() {
                        match kind {
                            Block::Struct(name) => c.structs.push((name, fields)),
                            Block::Event(code, name) => c.events.push((code, name, fields)),
                            Block::Skip => {}
                        }
                    }
                    continue;
                }
            }
            if !matches!(kind, Block::Skip) {
                if let Some(f) = parse_field(line) { fields.push(f); }
            }
            continue;
        }

        let words: Vec<&str> = line.split_whitespace().collect();
        if words.is_empty() { continue; }

        // cluster header: "[provisional|internal|deprecated]* cluster Name = N {"
        if let Some(pos) = words.iter().position(|w| *w == "cluster") {
            if words.get(pos + 2) == Some(&"=") && line.ends_with('{') {
                if let Some(c) = cur.take() { clusters.push(c); }
                cur = Some(Cluster {
                    name: words[pos + 1].to_string(),
                    code: words[pos + 3].trim_end_matches(['{', ' ']).parse().unwrap_or(u64::MAX),
                    ..Default::default()
                });
                continue;
            }
        }
        let Some(c) = cur.as_mut() else { continue };

        if line == "}" { clusters.push(cur.take().unwrap()); continue; }

        if words.contains(&"enum") || words.contains(&"bitmap") {
            if line.ends_with('{') { block = Some((Block::Skip, Vec::new())); depth_in_block = 1; }
            continue;
        }
        if let Some(pos) = words.iter().position(|w| *w == "struct") {
            // "request struct X {" | "response struct X = 8 {" | "struct X {" | "fabric_scoped struct X {"
            let name = words.get(pos + 1).unwrap_or(&"").trim_end_matches('{').to_string();
            if line.ends_with('{') { block = Some((Block::Struct(name), Vec::new())); depth_in_block = 1; }
            continue;
        }
        if words.iter().any(|w| *w == "event") {
            // "critical event StartUp = 0 {" | "fabric_sensitive info event access(read: administer) Foo = 0 {"
            // Qualifiers (critical/info/fabric_sensitive/provisional/...) and an
            // optional access(...) clause may appear between "event" and the
            // "Name = N {" part, so strip by literal split rather than fixed
            // word offsets.
            if line.ends_with('{') {
                let rest = line.split("event").nth(1).unwrap_or("");
                let rest = strip_access(rest);
                let rest = rest.trim().trim_end_matches('{').trim();
                if let Some((name_part, code_part)) = rest.rsplit_once('=') {
                    if let Ok(code) = code_part.trim().parse::<u64>() {
                        let name = name_part.trim().to_string();
                        block = Some((Block::Event(code, name), Vec::new()));
                        depth_in_block = 1;
                    }
                }
            }
            continue;
        }
        if words.contains(&"attribute") {
            // strip everything up to and including "attribute" and any "access(...)"
            let rest = line.split("attribute").nth(1).unwrap_or("");
            let rest = strip_access(rest);
            if let Some(f) = parse_field(&rest) { c.attrs.push(f); }
            continue;
        }
        if let Some(pos) = words.iter().position(|w| *w == "command") {
            // "[timed|fabric]* command [access(...)] Name(Input?): Output = N;"
            let is_timed = words[..pos].contains(&"timed");
            let rest = line.split("command").nth(1).unwrap_or("");
            let rest = strip_access(rest);
            if let Some(cap) = parse_command(&rest) {
                let (name, input, output, code) = cap;
                c.cmds.push((code, name, input, output, is_timed));
            }
            continue;
        }
    }
    if let Some(c) = cur.take() { clusters.push(c); }
    clusters.retain(|c| c.code != u64::MAX);
    clusters.sort_by_key(|c| c.code);
    clusters.dedup_by_key(|c| c.code);
    clusters
}

/// remove an "access(...)" group anywhere in the fragment
fn strip_access(s: &str) -> String {
    if let Some(start) = s.find("access(") {
        if let Some(end) = s[start..].find(')') {
            let mut out = String::new();
            out.push_str(&s[..start]);
            out.push_str(&s[start + end + 1..]);
            return out;
        }
    }
    s.to_string()
}

/// " Name(Input): Output = N;" -> (name, input, output, code)
fn parse_command(s: &str) -> Option<(String, Option<String>, Option<String>, u64)> {
    let s = s.trim().trim_end_matches(';');
    let (sig, code) = s.rsplit_once('=')?;
    let code: u64 = code.trim().parse().ok()?;
    let (call, output) = sig.rsplit_once(':')?;
    let output = output.trim();
    let output = if output == "DefaultSuccess" { None } else { Some(output.to_string()) };
    let call = call.trim();
    let open = call.find('(')?;
    let name = call[..open].trim().to_string();
    let inner = call[open + 1..call.rfind(')')?].trim();
    let input = if inner.is_empty() { None } else { Some(inner.to_string()) };
    Some((name, input, output, code))
}

fn render(clusters: &[Cluster]) -> String {
    let mut o = String::new();
    let esc = |s: &str| s.replace('"', "\\\"");
    let fields = |o: &mut String, fs: &[(u64, String, String, bool)]| {
        for (code, name, ty, is_list) in fs {
            writeln!(o, "        Field {{ code: {code}, name: \"{}\", ty: \"{}\", is_list: {is_list} }},", esc(name), esc(ty)).unwrap();
        }
    };
    writeln!(o, "static CLUSTERS: &[Cluster] = &[").unwrap();
    for c in clusters {
        writeln!(o, "Cluster {{ code: {}, name: \"{}\", attributes: &[", c.code, esc(&c.name)).unwrap();
        for (code, name, ty, is_list) in &c.attrs {
            writeln!(o, "    Attr {{ code: {code}, name: \"{}\", ty: \"{}\", is_list: {is_list} }},", esc(name), esc(ty)).unwrap();
        }
        writeln!(o, "], commands: &[").unwrap();
        for (code, name, input, output, timed) in &c.cmds {
            let i = input.as_ref().map(|s| format!("Some(\"{}\")", esc(s))).unwrap_or("None".into());
            let out = output.as_ref().map(|s| format!("Some(\"{}\")", esc(s))).unwrap_or("None".into());
            writeln!(o, "    Cmd {{ code: {code}, name: \"{}\", input: {i}, output: {out}, is_timed: {timed} }},", esc(name)).unwrap();
        }
        writeln!(o, "], structs: &[").unwrap();
        for (name, fs) in &c.structs {
            writeln!(o, "    Struct {{ name: \"{}\", fields: &[", esc(name)).unwrap();
            fields(&mut o, fs);
            writeln!(o, "    ] }},").unwrap();
        }
        writeln!(o, "], events: &[").unwrap();
        for (code, name, fs) in &c.events {
            writeln!(o, "    Event {{ code: {code}, name: \"{}\", fields: &[", esc(name)).unwrap();
            fields(&mut o, fs);
            writeln!(o, "    ] }},").unwrap();
        }
        writeln!(o, "] }},").unwrap();
    }
    writeln!(o, "];").unwrap();
    o
}
