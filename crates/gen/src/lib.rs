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

    /// `cluster()`'s `binary_search_by_key` is only correct on a table that is
    /// strictly ascending by code (which also rules out duplicate clusters).
    /// `build.rs` sorts, so this holds today — the test makes it a property rather
    /// than a coincidence, the way `vendors::table_is_strictly_ascending_by_id`
    /// does for the other generated table.
    #[test]
    fn table_is_strictly_ascending_by_code() {
        assert!(
            CLUSTERS.windows(2).all(|w| w[0].code < w[1].code),
            "CLUSTERS must be strictly ascending by code (also rules out duplicates)"
        );
    }

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

    #[test]
    fn access_control_event_with_access_clause_and_all_events_survive() {
        // AccessControl's events are declared as
        // "fabric_sensitive info event access(read: administer) Name = N {"
        // -- qualifiers *and* an access(...) clause between "event" and the
        // name/code. A parser that only handles "critical event Name = N {"
        // fails to open this event's block, and its closing "}" then gets
        // misread as the cluster's closing brace, truncating the rest of
        // the cluster (the remaining 3 events, later attributes, structs,
        // and the ReviewFabricRestrictions command all silently vanish).
        let c = cluster(31).expect("AccessControl");
        assert_eq!(c.events.len(), 4, "all 4 AccessControl events must survive parsing");

        let e = c.event(0).expect("AccessControlEntryChanged");
        assert_eq!(e.name, "AccessControlEntryChanged");
        // adminNodeID=1, adminPasscodeID=2, changeType=3, latestValue=4, fabricIndex=254
        assert_eq!(e.fields.len(), 5);
        assert!(e.fields.iter().any(|f| f.name == "adminNodeID" && f.code == 1));
        assert!(e.fields.iter().any(|f| f.name == "adminPasscodeID" && f.code == 2));
        assert!(e.fields.iter().any(|f| f.name == "changeType" && f.code == 3));
        assert!(e.fields.iter().any(|f| f.name == "latestValue" && f.code == 4));
        // Also guards the separate "fabric_idx is a type, not a qualifier" fix:
        // this field would be dropped entirely if fabric_idx were stripped.
        assert!(e.fields.iter().any(|f| f.name == "fabricIndex" && f.code == 254 && f.ty == "fabric_idx"));

        // The cluster body continues past this event: its command must
        // still be present, proving the cluster wasn't closed early.
        assert!(c.find_command_ci("reviewFabricRestrictions").is_some());
    }
}
