/**
 * SQLcl schema export script
 * - Exports DDL for a specific Oracle schema into ./schema_export/<SCHEMA>/*
 * - Works when connected as the schema owner (uses USER_OBJECTS)
 *   or as a privileged account (uses DBA_OBJECTS with OWNER filter).
 *
 * How to choose schema:
 *   1) ENV var:  EXPORT_SCHEMA=WS_PENSION_CALC
 *   2) Fallback: current connected user
 *
 * Notes:
 * - Uses DBMS_METADATA transforms for clean, source-control-friendly DDL
 * - Writes one file per object type/object
 */

var Paths = java.nio.file.Paths;
var Files = java.nio.file.Files;
var StandardOpenOption = java.nio.file.StandardOpenOption;
var Charset = java.nio.charset.StandardCharsets;

function up(s){ return (s || "").toUpperCase(); }

// --- Resolve schema to export ---
var TARGET = up(java.lang.System.getenv("EXPORT_SCHEMA"));
if (!TARGET || TARGET.trim() === "") {
  TARGET = up(util.getConn().getUser());
}
ctx.write("Exporting schema: " + TARGET + "\n");

// --- Root export folder: ./schema_export/<SCHEMA> ---
var ROOT = "schema_export/" + TARGET;

// --- Map Oracle object types to DBMS_METADATA types, folders, extensions ---
var MAP = {
  "TABLE"             : { ddl:"TABLE",              folder:"TABLES",               ext:".sql" },
  "VIEW"              : { ddl:"VIEW",               folder:"VIEWS",                ext:".sql" },
  "SEQUENCE"          : { ddl:"SEQUENCE",           folder:"SEQUENCES",            ext:".sql" },
  "INDEX"             : { ddl:"INDEX",              folder:"INDEXES",              ext:".sql" },
  "FUNCTION"          : { ddl:"FUNCTION",           folder:"FUNCTIONS",            ext:".pls" },
  "PROCEDURE"         : { ddl:"PROCEDURE",          folder:"PROCEDURES",           ext:".pls" },
  "PACKAGE"           : { ddl:"PACKAGE_SPEC",       folder:"PACKAGES",             ext:".pks" },
  "PACKAGE BODY"      : { ddl:"PACKAGE_BODY",       folder:"PACKAGES",             ext:".pkb" },
  "TRIGGER"           : { ddl:"TRIGGER",            folder:"TRIGGERS",             ext:".pls" },
  "TYPE"              : { ddl:"TYPE_SPEC",          folder:"TYPES",                ext:".pks" },
  "TYPE BODY"         : { ddl:"TYPE_BODY",          folder:"TYPES",                ext:".pkb" },
  "SYNONYM"           : { ddl:"SYNONYM",            folder:"SYNONYMS",             ext:".sql" },
  "MATERIALIZED VIEW" : { ddl:"MATERIALIZED_VIEW",  folder:"MATERIALIZED_VIEWS",   ext:".sql" }
};

// --- Tidy DDL output for version control ---
[
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'EMIT_SCHEMA',false)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'CONSTRAINTS',true)",
  "exec dbms_metadata.set_transform_param(dbms_metadata.session_transform,'REF_CONSTRAINTS',true)",
  "set long 2000000 pages 0 lines 32767 trimspool on"
].forEach(function(cmd){ sqlcl.setStmt(cmd); sqlcl.run(); });

// --- Ensure root exists ---
Files.createDirectories(Paths.get(ROOT));

// --- Build object list query depending on privileges/context ---
var currentUser = up(util.getConn().getUser());
var useUserObjects = (currentUser === TARGET); // connected as schema owner
var listSql;

if (useUserObjects) {
  listSql = ""
    + "select object_type, object_name "
    + "from user_objects "
    + "where temporary = 'N' "
    + "  and object_type in ("
    + "    'TABLE','VIEW','SEQUENCE','INDEX','FUNCTION','PROCEDURE',"
    + "    'PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','SYNONYM','MATERIALIZED VIEW'"
    + "  ) "
    + "order by object_type, object_name";
} else {
  // Requires SELECT on DBA_OBJECTS (e.g., SELECT_CATALOG_ROLE) if not the owner.
  listSql = ""
    + "select object_type, object_name "
    + "from dba_objects "
    + "where owner = :own "
    + "  and temporary = 'N' "
    + "  and object_type in ("
    + "    'TABLE','VIEW','SEQUENCE','INDEX','FUNCTION','PROCEDURE',"
    + "    'PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','SYNONYM','MATERIALIZED VIEW'"
    + "  ) "
    + "order by object_type, object_name";
}

var binds = {};
if (!useUserObjects) binds.own = TARGET;

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
  var ot = r.OBJECT_TYPE;
  var on = r.OBJECT_NAME;
  if (!MAP[ot]) return;

  var m = MAP[ot];
  var ddlSql;

  // Always pass OWNER => TARGET to get DDL for the requested schema
  ddlSql = "select dbms_metadata.get_ddl('" + m.ddl + "', :obj, :own) from dual";

  var b = { obj: on, own: TARGET };
  var ddl = util.executeReturnClob(ddlSql, b);

  var dir = ROOT + "/" + m.folder;
  var file = dir + "/" + on + m.ext;

  writeFile(file, ddl);
});

// Optional: export object grants (uncomment to enable)
/*
var grantsDir = ROOT + "/GRANTS";
var grants = util.executeReturnClob(
  "select dbms_metadata.get_granted_ddl('OBJECT_GRANT', :own) from dual",
  { own: TARGET }
);
writeFile(grantsDir + "/object_grants.sql", grants);
*/

// Done
ctx.write("Export complete: " + TARGET + " → " + ROOT + "\n");