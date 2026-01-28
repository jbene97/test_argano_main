/*
 * Export WKSP_PENSIONCALC objects into schema_export/WKSP_PENSIONCALC/<TYPE>/<OBJECT>.*
 * Designed to run under SQLcl (sql /nolog @thisfile)
 */

var Paths = java.nio.file.Paths;
var Files = java.nio.file.Files;

function up(s){ return (s||'').toUpperCase(); }
function stripQuotes(s){ return (s||'').replace(/^"|"$/g,''); }

var TARGET = 'WKSP_PENSIONCALC';
var ROOT = 'schema_export/' + TARGET;
var WORK = java.lang.System.getenv('GITHUB_WORKSPACE') || java.lang.System.getProperty('user.dir');

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

ctx.write('Exporting schema: ' + TARGET + '\n');

// SQLcl display tweaks
["set long 2000000","set linesize 32767","set pagesize 0","set trimspool on"].forEach(function(c){ sqlcl.setStmt(c); sqlcl.run(); });

// Build object list by spooling a CSV
var listDir = WORK + '/' + ROOT;
Files.createDirectories(Paths.get(listDir));
var listFile = listDir + '/.object_list.csv';

sqlcl.setStmt('set heading off'); sqlcl.run();
sqlcl.setStmt('set sqlformat csv'); sqlcl.run();

var listSql =
  "select object_type,object_name from dba_objects where owner='" + TARGET + "' and temporary='N' and object_type in (" +
  "'TABLE','VIEW','SEQUENCE','INDEX','FUNCTION','PROCEDURE','PACKAGE','PACKAGE BODY','TRIGGER','TYPE','TYPE BODY','SYNONYM','MATERIALIZED VIEW') order by object_type,object_name";

sqlcl.setStmt('spool "' + listFile + '"'); sqlcl.run();
sqlcl.setStmt(listSql); sqlcl.run();
sqlcl.setStmt('spool off'); sqlcl.run();

// read list
var csv = '';
try {
  var bytes = Files.readAllBytes(Paths.get(listFile));
  csv = String(new java.lang.String(bytes, java.nio.charset.StandardCharsets.UTF_8)).trim();
} catch (e) {
  csv = '';
}

sqlcl.setStmt('set sqlformat ansiconsole'); sqlcl.run();

var lines = csv ? csv.split(/\r?\n/) : [];
var count = 0;

// Pre-set DBMS_METADATA session transforms for cleaner DDL
try {
  var setTransforms = "begin\n" +
    " dbms_metadata.set_transform_param(dbms_metadata.session_transform,'STORAGE',false);\n" +
    " dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',false);\n" +
    " dbms_metadata.set_transform_param(dbms_metadata.session_transform,'TABLESPACE',false);\n" +
    " dbms_metadata.set_transform_param(dbms_metadata.session_transform,'EMIT_SCHEMA',false);\n" +
    "end;";
  sqlcl.setStmt(setTransforms); sqlcl.run();
} catch (e) { /* ignore */ }

lines.forEach(function(line){
  line = (line||'').trim();
  if (!line) return;

  var parts = line.split(',');
  if (parts.length < 2) return;

  var ot = stripQuotes(parts[0].trim()).toUpperCase();
  var on = stripQuotes(parts[1].trim());
  if (!MAP[ot]) return;

  var dirRel = ROOT + '/' + MAP[ot].folder;
  var absDir = WORK + '/' + dirRel;
  Files.createDirectories(Paths.get(absDir));

  var absFile = WORK + '/' + dirRel + '/' + on + MAP[ot].ext;

  // spool DBMS_METADATA.get_ddl(...) into the file
  var spoolOn = 'spool "' + absFile + '"';
  var ddlSelect = "select dbms_metadata.get_ddl('" + TARGET.replace("'","''") + "','" + on.replace("'","''") + "','" + TARGET + "') from dual";
  var spoolOff = 'spool off';

  ctx.write('Spooling DDL for ' + TARGET + '.' + on + ' to ' + absFile + '\n');
  sqlcl.setStmt(spoolOn); sqlcl.run();
  sqlcl.setStmt(ddlSelect); sqlcl.run();
  sqlcl.setStmt(spoolOff); sqlcl.run();

  // verify
  var saved = false;
  try { var p = Paths.get(absFile); if (Files.exists(p) && Files.size(p) > 0) saved = true; } catch(e){}

  if (!saved) {
    // retry without owner
    try {
      ctx.write('Retrying without owner for ' + on + '\n');
      sqlcl.setStmt('spool "' + absFile + '"'); sqlcl.run();
      sqlcl.setStmt("select dbms_metadata.get_ddl('" + TARGET.replace("'","''") + "','" + on.replace("'","''") + "') from dual"); sqlcl.run();
      sqlcl.setStmt('spool off'); sqlcl.run();
      var p2 = Paths.get(absFile); if (Files.exists(p2) && Files.size(p2) > 0) saved = true;
    } catch(e){}
  }

  if (!saved) {
    ctx.write('Warning: failed to extract DDL for ' + TARGET + '.' + on + '\n');
  } else {
    ctx.write('Saved: ' + absFile + '\n');
    count++;
  }
});

ctx.write('Export complete: ' + TARGET + ' -> ' + ROOT + ' (' + count + ' objects)\n');

