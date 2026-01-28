/**
 * Export DDL for a specific schema into ./schema_export/<SCHEMA>/*
 * Fixes:
 *  - Use SET LINESIZE / PAGESIZE (not LINES/PAGES) to avoid SP2-0158.
 *  - Replace util.getConn() with util.executeReturnOneCol('select user from dual').
 */

var Paths = java.nio.file.Paths;
var Files = java.nio.file.Files;
var StandardOpenOption = java.nio.file.StandardOpenOption;
var Charset = java.nio.charset.StandardCharsets;

function up(s){ return (s || "").toUpperCase(); }

// --- Resolve schema to export ---
var TARGET = up(java.lang.System.getenv("EXPORT_SCHEMA"));
if (!TARGET || TARGET.trim() === "") {
  // No getConn() helper; use SQL to get the current user
  TARGET = up( util.executeReturnOneCol("select user from dual") );
}
ctx.write("Exporting schema: " + TARGET + "\n");

// --- Root export folder ---
var ROOT = "schema_export/" + TARGET;

// --- Map types to DBMS_METADATA types / folders / extensions ---
var MAP = {
  "TABLE"             : { ddl:"TABLE",              folder:"TABLES",               ext:".sql" },
  "VIEW"              : { ddl:"VIEW",               folder:"VIEWS",                ext:".sql" },
  "SEQUENCE"          : { ddl:"SEQUENCE",           folder:"SEQUENCES",            ext:".sql" },
  "INDEX"             : { ddl:"INDEX",              folder:"INDEXES",              ext:".sql" },
  "FUNCTION"          : { ddl:"FUNCTION",           folder:"FUNCTIONS",            ext:".pls" },
  "PROCEDURE"         : { ddl:"PROCEDURES",         folder:"PROCEDURES",           ext:".pls" }, // folder was pluralized
  "PACKAGE"           : { ddl:"PACKAGE_SPEC",       folder:"PACKAGES",             ext:".pks" },
  "PACKAGE BODY"      : { ddl:"PACKAGE_BODY",       folder:"PACKAGES",             ext:".pkb" },
  "TRIGGER"           : { ddl:"TRIGGER",            folder:"TRIGGERS",             ext:".pls" },
  "TYPE"              : { ddl:"TYPE_SPEC",          folder:"TYPES",                ext:".pks" },
  "TYPE BODY"         : { ddl:"TYPE_BODY",          folder:"TYPES",                ext:".pkb" },
  "SYNONYM"           : { ddl:"SYNONYM",            folder:"SYNONYMS",             ext:".sql" },
  "MATERIALIZED VIEW" : { ddl:"MATERIALIZED_VIEW",  folder:"MATERIALIZED_VIEWS",   ext:".sql" }
};

// --- Clean DDL output for version control ---
[
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'EMIT_SCHEMA',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS',true)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'REF_CONSTRAINTS',true)",
  // SQL*Plus/SQLcl display settings (use LINESIZE/PAGESIZE)
  "set long 2000000",
  "set linesize 32767",
  "set pagesize 0",
  "set trimspool on"
].forEach(function(cmd){ sqlcl.setStmt(cmd); sqlcl.run(); });

// --- Ensure root exists ---
Files.createDirectories(Paths.get(ROOT));

// --- Build object list (use USER_OBJECTS if we are the owner; else DBA_OBJECTS with OWNER filter) ---
var currentUser = up( util.executeReturnOneCol("select user from dual") );
var useUserObjects = (currentUser === TARGET);

var listSql, binds = {};
if (useUserObjects) {
  listSql =
    "select object_type, object_name " +
    "from user_objects " +
    "where temporary = 'N' " +
    "  and object_type in (" +
    "    'TABLE','VIEW','SEQUENCE','INDEX','FUNCTION','PROCEDURE'," +
    "    'PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','SYNONYM','MATERIALIZED VIEW'" +
    "  ) " +
    "order by object_type, object_name";
} else {
  listSql =
    "select object_type, object_name " +
    "from dba_objects " +
    "where owner = :own " +
    "  and temporary = 'N' " +
    "  and object_type in (" +
    "    'TABLE','VIEW','SEQUENCE','INDEX','FUNCTION','PROCEDURE'," +
    "    'PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','SYNONYM','MATERIALIZED VIEW'" +
    "  ) " +
    "order by object_type, object_name";
  binds.own = TARGET;
}

var rows = util.executeReturnList(listSql, binds);

function writeFile(pathStr, content) {
  var p = Paths.get(pathStr);
  Files.createDirectories(p.getParent());
  Files.write(
    p,
    String(content || "").getBytes(Charset.UTF_8),
    StandardOpenOption.CREATE,
    StandardOpenOption.TRUNCATE_EXISTING
  );
}

rows.forEach(function(r) {
  var ot = r.OBJECT_TYPE, on = r.OBJECT_NAME;
  if (!MAP[ot]) return;

  var m = MAP[ot];
  var ddl = util.executeReturnClob(
    "select dbms_metadata.get_ddl(:t, :o, :own) from dual",
    { t: m.ddl, o: on, own: TARGET }
  );

  var dir = ROOT + "/" + m.folder;
  var file = dir + "/" + on + m.ext;
  writeFile(file, ddl);
});

// Done
ctx.write("Export complete: " + TARGET + " → " + ROOT + "\n");