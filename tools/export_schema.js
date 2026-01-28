// File: tools/export_schema.js
// Exports the connected schema's objects into ./schema_export/*
// Requires: SQLcl, DBMS_METADATA. See AskTom & SQLcl docs for transform settings.  [2](https://github.com/oracle-quickstart/oci-github-actions-runner)[1](https://github.com/oracle-actions)

var Paths = java.nio.file.Paths;
var Files = java.nio.file.Files;
var StandardOpenOption = java.nio.file.StandardOpenOption;

// Root export folder
var ROOT = "schema_export";

// Object type mapping: DBMS_METADATA type, folder, extension
var MAP = {
  "TABLE":          { ddl:"TABLE",          folder:"TABLES",      ext:".sql" },
  "VIEW":           { ddl:"VIEW",           folder:"VIEWS",       ext:".sql" },
  "SEQUENCE":       { ddl:"SEQUENCE",       folder:"SEQUENCES",   ext:".sql" },
  "INDEX":          { ddl:"INDEX",          folder:"INDEXES",     ext:".sql" },
  "FUNCTION":       { ddl:"FUNCTION",       folder:"FUNCTIONS",   ext:".pls" },
  "PROCEDURE":      { ddl:"PROCEDURE",      folder:"PROCEDURES",  ext:".pls" },
  "PACKAGE":        { ddl:"PACKAGE_SPEC",   folder:"PACKAGES",    ext:".pks" },
  "PACKAGE BODY":   { ddl:"PACKAGE_BODY",   folder:"PACKAGES",    ext:".pkb" },
  "TRIGGER":        { ddl:"TRIGGER",        folder:"TRIGGERS",    ext:".pls" },
  "TYPE":           { ddl:"TYPE_SPEC",      folder:"TYPES",       ext:".pks" },
  "TYPE BODY":      { ddl:"TYPE_BODY",      folder:"TYPES",       ext:".pkb" },
  "SYNONYM":        { ddl:"SYNONYM",        folder:"SYNONYMS",    ext:".sql" },
  "MATERIALIZED VIEW": { ddl:"MATERIALIZED_VIEW", folder:"MATERIALIZED_VIEWS", ext:".sql" }
};

// ---- Tidy DDL output: disable noise, include constraints in table DDL ----
// These transforms are the standard way to make DDL source-control friendly.  [2](https://github.com/oracle-quickstart/oci-github-actions-runner)
[
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'EMIT_SCHEMA',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS',true)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'REF_CONSTRAINTS',true)",
  "set long 2000000 pages 0 lines 32767 trimspool on"
].forEach(function(cmd){ sqlcl.setStmt(cmd); sqlcl.run(); });

// Make root dir
Files.createDirectories(Paths.get(ROOT));

// Fetch objects in the connected schema
// Filter out Oracle-maintained & non-source objects as needed.
var q = ""
+ "select object_type, object_name "
+ "from user_objects "
+ "where object_type in ("
+ " 'TABLE','VIEW','SEQUENCE','INDEX','FUNCTION','PROCEDURE',"
+ " 'PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','SYNONYM','MATERIALIZED VIEW'"
+ ") "
+ "and temporary = 'N' "
+ "order by object_type, object_name";

var rows = util.executeReturnList(q);

rows.forEach(function(r) {
  var ot = r.OBJECT_TYPE, on = r.OBJECT_NAME;
  if (!MAP[ot]) return;

  var m = MAP[ot];
  var ddl = util.executeReturnClob(
    "select dbms_metadata.get_ddl('" + m.ddl + "','" + on + "') from dual"
  );

  var dir = ROOT + "/" + m.folder;
  Files.createDirectories(Paths.get(dir));
  var file = dir + "/" + on + m.ext;

  Files.write(
    Paths.get(file),
    String(ddl).getBytes(java.nio.charset.StandardCharsets.UTF_8),
    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
  );
});

// (Optional) export object grants, system/role grants, synonyms pointing to other schemas, etc.
// You can add calls to DBMS_METADATA.GET_GRANTED_DDL/GET_DEPENDENT_DDL if desired.  [2](https://github.com/oracle-quickstart/oci-github-actions-runner)