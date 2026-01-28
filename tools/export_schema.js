/**
 * Export DDL for a specific schema into ./schema_export/<SCHEMA>/*
 * GraalVM-friendly version: no executeReturnClob; uses SQLcl's DDL ... SAVE
 * Requires: SQLcl; Java 17+ with GraalVM JS (or Java 11 Nashorn)
 *
 * IMPORTANT:
 *   - Set ENV: EXPORT_SCHEMA=WKSP_PENSIONCALC
 *   - Connect as the OWNER (WKSP_PENSIONCALC) OR as a user with SELECT_CATALOG_ROLE
 *     when exporting another schema (for DBA_OBJECTS access).
 */

var Paths = java.nio.file.Paths;
var Files = java.nio.file.Files;
var StandardOpenOption = java.nio.file.StandardOpenOption;

function up(s){ return (s || "").toUpperCase(); }

// Safe output reader: support multiple SQLcl/Graal/Nashorn variants
function readOutput() {
  try {
    if (typeof ctx !== 'undefined' && typeof ctx.getOutput === 'function') return String(ctx.getOutput());
    if (typeof ctx !== 'undefined' && ctx.getOutput != null) return String(ctx.getOutput);
    if (typeof ctx !== 'undefined' && typeof ctx.getStdout === 'function') return String(ctx.getStdout());
    if (typeof sqlcl !== 'undefined' && typeof sqlcl.getOutput === 'function') return String(sqlcl.getOutput());
    if (typeof GetOutput === 'function') return String(GetOutput());
    if (typeof getOutput === 'function') return String(getOutput());
  } catch (e) {
    // ignore and return empty
  }
  return "";
}

// ----------------------------------------------------------------------------
// Resolve target schema (env var or fallback to connected user via SELECT USER)
// ----------------------------------------------------------------------------
var TARGET = up(java.lang.System.getenv("EXPORT_SCHEMA"));
if (!TARGET || TARGET.trim() === "") {
  // Fallback: read current user using SQLcl output
  sqlcl.setStmt("select user from dual");
  sqlcl.run();
  var out = String(readOutput()).trim();
  TARGET = up(out.split(/\s+/).pop()); // last token is the USER
}
ctx.write("Exporting schema: " + TARGET + "\n");

// Root export folder
var ROOT = "schema_export/" + TARGET;

// Type → folder, extension
var MAP = {
  "TABLE"             : { folder:"TABLES",               ext:".sql" },
  "VIEW"              : { folder:"VIEWS",                ext:".sql" },
  "SEQUENCE"          : { folder:"SEQUENCES",            ext:".sql" },
  "INDEX"             : { folder:"INDEXES",              ext:".sql" },
  "FUNCTION"          : { folder:"FUNCTIONS",            ext:".pls" },
  "PROCEDURE"         : { folder:"PROCEDURES",           ext:".pls" },
  "PACKAGE"           : { folder:"PACKAGES",             ext:".pks" },
  "PACKAGE BODY"      : { folder:"PACKAGES",             ext:".pkb" },
  "TRIGGER"           : { folder:"TRIGGERS",             ext:".pls" },
  "TYPE"              : { folder:"TYPES",                ext:".pks" },
  "TYPE BODY"         : { folder:"TYPES",                ext:".pkb" },
  "SYNONYM"           : { folder:"SYNONYMS",             ext:".sql" },
  "MATERIALIZED VIEW" : { folder:"MATERIALIZED_VIEWS",   ext:".sql" }
};

// ----------------------------------------------------------------------------
// Clean DDL for version control (SQLcl supports concise DDL switches)
// ----------------------------------------------------------------------------
[
  "set ddl storage off",
  "set ddl segment_attributes off",
  "set ddl tablespace off",
  "set ddl emit_schema off",
  "set ddl constraints on",
  "set ddl ref_constraints on",
  "set long 2000000",
  "set linesize 32767",
  "set pagesize 0",
  "set trimspool on"
].forEach(function(cmd){ sqlcl.setStmt(cmd); sqlcl.run(); });
// (SQLcl DDL switches from Jeff Smith’s notes)  // documentation ref in response

// ----------------------------------------------------------------------------
// Determine connected user
// ----------------------------------------------------------------------------
sqlcl.setStmt("select user from dual");
sqlcl.run();
var CURRENT = up(String(readOutput()).trim().split(/\s+/).pop());

// ----------------------------------------------------------------------------
// Build list of objects via SQL and capture the rows using SQLcl output
// (We print as: TYPE|NAME  to parse reliably.)
// ----------------------------------------------------------------------------
var listSql;
if (CURRENT === TARGET) {
  listSql =
    "select object_type || '|' || object_name line " +
    "from user_objects " +
    "where temporary='N' and object_type in (" +
    " 'TABLE','VIEW','SEQUENCE','INDEX','FUNCTION','PROCEDURE'," +
    " 'PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','SYNONYM','MATERIALIZED VIEW')" +
    " order by object_type, object_name";
} else {
  listSql =
    "select object_type || '|' || object_name line " +
    "from dba_objects " +
    "where owner = '" + TARGET + "' and temporary='N' and object_type in (" +
    " 'TABLE','VIEW','SEQUENCE','INDEX','FUNCTION','PROCEDURE'," +
    " 'PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','SYNONYM','MATERIALIZED VIEW')" +
    " order by object_type, object_name";
}

sqlcl.setStmt(listSql);
sqlcl.run();
var lines = String(readOutput()).trim().split(/\r?\n/);

// Ensure root
Files.createDirectories(Paths.get(ROOT));

var count = 0;

// ----------------------------------------------------------------------------
// For each object, call SQLcl DDL with SAVE to write directly to disk.
// ----------------------------------------------------------------------------
lines.forEach(function(row) {
  row = row.trim();
  if (!row || row.indexOf('|') < 0) return;

  var parts = row.split('|');
  var ot = up(parts[0].trim());
  var on = parts[1].trim();

  if (!MAP[ot]) return;

  var dir = ROOT + "/" + MAP[ot].folder;
  var file = dir + "/" + on + MAP[ot].ext;

  // create folder(s)
  Files.createDirectories(Paths.get(dir));

  // Build fully-qualified object reference when CURRENT != TARGET
  var objRef = (CURRENT === TARGET) ? on : (TARGET + "." + on);

  // Use SQLcl DDL to write DDL to file
  // Syntax: DDL <object> <type> SAVE <file>
  // (Type is optional, but passing it makes SQLcl explicit.)
  var cmd = "ddl " + objRef + " " + ot + " save " + file;
  sqlcl.setStmt(cmd);
  sqlcl.run();

  count++;
});

// Done
ctx.write("Export complete: " + TARGET + " → " + ROOT + " (" + count + " objects)\n");