//! Build-time cluster metadata from the CSA .matter IDL (vendored V1.6.0.0).
//! Used for: device_command name->id, name-based JSON for command responses
//! and events, and TLV type hints for JSON->TLV encoding.

#[derive(Debug)]
pub struct Cluster {
    pub code: u32,
    pub name: &'static str,
    pub attributes: &'static [Attr],
    pub commands: &'static [Cmd],
    pub structs: &'static [Struct],
    pub events: &'static [Event],
}

#[derive(Debug)]
pub struct Attr { pub code: u32, pub name: &'static str, pub ty: &'static str, pub is_list: bool }
#[derive(Debug)]
pub struct Cmd { pub code: u32, pub name: &'static str, pub input: Option<&'static str>, pub output: Option<&'static str>, pub is_timed: bool }
#[derive(Debug)]
pub struct Struct { pub name: &'static str, pub fields: &'static [Field] }
#[derive(Debug)]
pub struct Field { pub code: u32, pub name: &'static str, pub ty: &'static str, pub is_list: bool }
#[derive(Debug)]
pub struct Event { pub code: u32, pub name: &'static str, pub fields: &'static [Field] }

include!(concat!(env!("OUT_DIR"), "/tables.rs")); // defines: static CLUSTERS: &[Cluster] (sorted by code)

/// Look up a cluster by its Matter cluster id.
pub fn cluster(code: u32) -> Option<&'static Cluster> {
    CLUSTERS.binary_search_by_key(&code, |c| c.code).ok().map(|i| &CLUSTERS[i])
}

impl Cluster {
    pub fn find_command_ci(&self, name: &str) -> Option<&'static Cmd> {
        self.commands.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
    pub fn find_struct(&self, name: &str) -> Option<&'static Struct> {
        self.structs.iter().find(|s| s.name == name)
    }
    pub fn attr(&self, code: u32) -> Option<&'static Attr> {
        self.attributes.iter().find(|a| a.code == code)
    }
    pub fn event(&self, code: u32) -> Option<&'static Event> {
        self.events.iter().find(|e| e.code == code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn onoff_commands() {
        let c = cluster(6).expect("OnOff cluster");
        assert_eq!(c.name, "OnOff");
        let toggle = c.find_command_ci("toggle").expect("Toggle");
        assert_eq!(toggle.code, 2);
        assert_eq!(toggle.input, None);
        assert_eq!(toggle.output, None); // DefaultSuccess
    }

    #[test]
    fn level_control_input_struct_fields() {
        let c = cluster(8).expect("LevelControl");
        let mv = c.find_command_ci("moveToLevel").expect("MoveToLevel");
        let input = c.find_struct(mv.input.expect("has input")).expect("request struct");
        let level = input.fields.iter().find(|f| f.name == "level").unwrap();
        assert_eq!(level.code, 0);
        assert_eq!(level.ty, "int8u");
    }

    #[test]
    fn operational_credentials_response_struct() {
        let c = cluster(62).expect("OperationalCredentials");
        let rf = c.find_command_ci("removeFabric").expect("RemoveFabric");
        assert_eq!(rf.code, 10);
        let out = c.find_struct(rf.output.expect("NOCResponse")).unwrap();
        assert!(out.fields.iter().any(|f| f.name == "statusCode" && f.code == 0));
        assert!(out.fields.iter().any(|f| f.name == "fabricIndex" && f.code == 1));
    }

    #[test]
    fn admin_commissioning_is_timed() {
        let c = cluster(60).expect("AdministratorCommissioning");
        let ocw = c.find_command_ci("openCommissioningWindow").unwrap();
        assert!(ocw.is_timed);
    }

    #[test]
    fn descriptor_device_type_list_is_list_attr() {
        let c = cluster(29).expect("Descriptor");
        let a = c.attr(0).unwrap();
        assert_eq!(a.name, "deviceTypeList");
        assert!(a.is_list);
    }

    #[test]
    fn events_with_fields() {
        // BasicInformation StartUp event carries softwareVersion.
        let c = cluster(40).unwrap();
        let e = c.event(0).expect("StartUp");
        assert_eq!(e.name, "StartUp");
        assert!(e.fields.iter().any(|f| f.name == "softwareVersion"));
    }
}
