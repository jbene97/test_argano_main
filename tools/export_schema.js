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

// ---- Emit the list as CSV without headers, then parse ----
sqlcl.setStmt("set heading off");
sqlcl.run();
sqlcl.setStmt("set sqlformat csv");
sqlcl.run();

var listSql;
if (CURRENT === TARGET) {
  listSql =
    "select object_type, object_name " +
    "from user_objects " +
    "where temporary='N' and object_type in (" +
    " 'TABLE','VIEW','SEQUENCE','INDEX','FUNCTION','PROCEDURE'," +
    " 'PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','SYNONYM','MATERIALIZED VIEW')" +
    " order by object_type, object_name";
} else {
  listSql =
    "select object_type, object_name " +
    "from dba_objects " +
    "where owner = '" + TARGET + "' and temporary='N' and object_type in (" +
    " 'TABLE','VIEW','SEQUENCE','INDEX','FUNCTION','PROCEDURE'," +
    " 'PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','SYNONYM','MATERIALIZED VIEW')" +
    " order by object_type, object_name";
}

// Spool the object list to a file to reliably capture SQLcl output in CI
var WORK = java.lang.System.getenv("GITHUB_WORKSPACE");
var listDir = (WORK ? WORK + "/" : "") + ROOT;
Files.createDirectories(Paths.get(listDir));
var listFile = listDir + "/.object_list.csv";

sqlcl.setStmt('spool "' + listFile + '"');
sqlcl.run();
sqlcl.setStmt(listSql);
sqlcl.run();
sqlcl.setStmt('spool off');
sqlcl.run();

// Read the spooled file from disk
var csv = "";
try {
  var bytes = Files.readAllBytes(Paths.get(listFile));
  csv = String(new java.lang.String(bytes, java.nio.charset.StandardCharsets.UTF_8)).trim();
} catch (e) {
  csv = String(readOutput()).trim(); // fallback to previous approach
}

// Restore pretty output for the rest if you want
sqlcl.setStmt("set sqlformat ansiconsole");
sqlcl.run();

var lines = csv ? csv.split(/\r?\n/) : [];

var count = 0;
lines.forEach(function(line) {
  line = line.trim();
  if (!line) return;

  // Format is: OBJECT_TYPE,OBJECT_NAME
  var parts = line.split(",");
  if (parts.length < 2) return;

  var ot = parts[0].trim().toUpperCase();
  var on = parts[1].trim();

  if (!MAP[ot]) return;

  var folder = MAP[ot].folder;
  var ext    = MAP[ot].ext;

  var dirRel = ROOT + "/" + folder;
  var fileRel = dirRel + "/" + on + ext;

  // ensure dirs exist in the repo (create absolute path inside workspace)
  var absDir = (WORK ? WORK + "/" : "") + dirRel;
  Files.createDirectories(Paths.get(absDir));

  // If not connected as owner, fully-qualify the object for DDL
  var objRef = (CURRENT === TARGET) ? on : (TARGET + "." + on);

  // Use an absolute file path for SAVE to avoid CWD confusion
  var absFile = (WORK ? WORK + "/" : "") + fileRel;
  // Use DBMS_METADATA.GET_DDL and spool its output to the target file. This
  // is more reliable in CI than parsing SQLcl stdout and gives us the actual
  // DDL from the database.

  // Ensure DBMS_METADATA transforms are set for consistent output (no storage/tablespace)
  try {
    var setTransforms = "begin dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',false); dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',false); dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',false); dbms_metadata.set_transform_param(dbms_metadata.session_transform,'EMIT_SCHEMA',false); end;";
    sqlcl.setStmt(setTransforms);
    sqlcl.run();
  } catch (e) { /* ignore */ }

  // Prepare SQL to spool the DDL to the file
  var spoolOn = 'spool "' + absFile + '"';
  var ddlSelect = "select dbms_metadata.get_ddl('" + ot.replace("'","''") + "','" + on.replace("'","''") + "','" + TARGET + "') from dual";
  var spoolOff = 'spool off';

  ctx.write("Spooling DDL for " + objRef + " to " + absFile + "\n");
  sqlcl.setStmt(spoolOn); sqlcl.run();
  sqlcl.setStmt(ddlSelect); sqlcl.run();
  sqlcl.setStmt(spoolOff); sqlcl.run();

  // Verify the file was written and non-empty
  var saved = false;
  try {
    var p = Paths.get(absFile);
    if (Files.exists(p) && Files.size(p) > 0) saved = true;
  } catch (e) {}

  if (!saved) {
    // Retry: some objects (like PACKAGE BODY) may need alternate handling; try without owner
    try {
      ctx.write("Retrying DDL without owner for " + objRef + "\n");
      sqlcl.setStmt('spool "' + absFile + '"'); sqlcl.run();
      sqlcl.setStmt("select dbms_metadata.get_ddl('" + ot.replace("'","''") + "','" + on.replace("'","''") + "') from dual"); sqlcl.run();
      sqlcl.setStmt('spool off'); sqlcl.run();
      var p2 = Paths.get(absFile);
      if (Files.exists(p2) && Files.size(p2) > 0) saved = true;
    } catch (e) {}
  }

  if (!saved) {
    ctx.write("Warning: DDL extract failed for " + objRef + " -> " + absFile + "\n");
  } else {
    ctx.write("Saved: " + absFile + "\n");
    count++;
  }
});
ctx.write("Export complete: " + TARGET + " → " + ROOT + " (" + count + " objects)\n");
// (CSV-driven export already completed above)