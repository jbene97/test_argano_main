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
["set long 2000000","set linesize 32767","set pagesize 0","set trimspool on","set serveroutput on size 1000000"].forEach(function(c){ sqlcl.setStmt(c); sqlcl.run(); });

// Build object list by spooling a CSV
var listDir = WORK + '/' + ROOT;
Files.createDirectories(Paths.get(listDir));
var listFile = listDir + '/.object_list.csv';

sqlcl.setStmt('set heading off'); sqlcl.run();
sqlcl.setStmt('set sqlformat csv'); sqlcl.run();

var listSql =
  "select object_type,object_name,status from dba_objects where owner='" + TARGET + "' and temporary='N' and object_type in (" +
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
  if (parts.length < 3) return;

  var ot = stripQuotes(parts[0].trim()).toUpperCase();
  var on = stripQuotes(parts[1].trim());
  var status = stripQuotes(parts[2].trim()).toUpperCase();
  if (!MAP[ot]) return;

  // Skip obvious system-generated / internal names to avoid failures and noise
  if (/^(SYS_|ISEQ\$\$|BIN\$)/.test(on) || on.indexOf('SYS_IL') === 0 || on.indexOf('$$') !== -1) {
    ctx.write('Skipping internal/system object: ' + on + '\n');
    return;
  }

  var dirRel = ROOT + '/' + MAP[ot].folder;
  var absDir = WORK + '/' + dirRel;
  Files.createDirectories(Paths.get(absDir));

  // sanitize filename to avoid SQLcl/SP2 spool issues (e.g. $$) and shell problems
  var safeName = on.replace(/[^A-Za-z0-9_.-]/g, '_');
  var absFile = WORK + '/' + dirRel + '/' + safeName + MAP[ot].ext;

  // spool a PL/SQL block that safely attempts GET_DDL and falls back to ALL_SOURCE
  var spoolOn = 'spool "' + absFile + '"';
  var spoolOff = 'spool off';

  // escape single quotes for PL/SQL
  var ot_esc = ot.replace("'","''");
  var on_esc = on.replace("'","''");

  var plsql = '';
  plsql += "DECLARE\n";
  plsql += "  l_ddl CLOB;\n";
  plsql += "  l_pos INTEGER := 1;\n";
  plsql += "  l_len INTEGER;\n";
  plsql += "  l_chunk VARCHAR2(32767);\n";
  plsql += "BEGIN\n";
  plsql += "  BEGIN\n";
  plsql += "   /* l_ddl := DBMS_METADATA.GET_DDL('" + ot_esc + "','" + on_esc + "','" + TARGET + "');\n";
  plsql += "    IF l_ddl IS NOT NULL THEN\n";
  plsql += "      l_len := DBMS_LOB.GETLENGTH(l_ddl);\n";
  plsql += "      WHILE l_pos <= l_len LOOP\n";
  plsql += "        l_chunk := DBMS_LOB.SUBSTR(l_ddl,32767,l_pos);\n";
  plsql += "        DBMS_OUTPUT.PUT_LINE(l_chunk);\n";
  plsql += "        l_pos := l_pos + 32767;\n";
  plsql += "      END LOOP;\n";
  plsql += "    END IF;*/\n";
  plsql += "    FOR r IN (SELECT text FROM all_source WHERE owner='" + TARGET + "' AND name='" + on_esc + "' ORDER BY line) LOOP\n";
  plsql += "      DBMS_OUTPUT.PUT_LINE(r.text);\n";
  plsql += "    END LOOP;\n";
  plsql += "  EXCEPTION WHEN OTHERS THEN\n";
  plsql += "    DBMS_OUTPUT.PUT_LINE('--ERROR:'||SQLERRM);\n";
  plsql += "    FOR r IN (SELECT text FROM all_source WHERE owner='" + TARGET + "' AND name='" + on_esc + "' ORDER BY line) LOOP\n";
  plsql += "      DBMS_OUTPUT.PUT_LINE(r.text);\n";
  plsql += "    END LOOP;\n";
  plsql += "  END;\n";
  plsql += "END;\n/";

  ctx.write('Spooling DDL for ' + TARGET + '.' + on + ' -> ' + absFile + '\n');
  // disable echo so the PL/SQL block itself isn't written into the spool
  try { sqlcl.setStmt('set echo off'); sqlcl.run(); } catch(e) {}
  sqlcl.setStmt(spoolOn); sqlcl.run();
  sqlcl.setStmt(plsql); sqlcl.run();
  sqlcl.setStmt(spoolOff); sqlcl.run();
  // restore echo
  try { sqlcl.setStmt('set echo on'); sqlcl.run(); } catch(e) {}

  // verify
  var saved = false;
  try { var p = Paths.get(absFile); if (Files.exists(p) && Files.size(p) > 0) saved = true; } catch(e){}

  if (!saved) {
    // retry without owner
    try {
      ctx.write('Retrying without owner for ' + on + '\n');
      sqlcl.setStmt('spool "' + absFile + '"'); sqlcl.run();
      sqlcl.setStmt("select dbms_metadata.get_ddl('" + ot.replace("'","''") + "','" + on.replace("'","''") + "') from dual"); sqlcl.run();
      sqlcl.setStmt('spool off'); sqlcl.run();
      var p2 = Paths.get(absFile); if (Files.exists(p2) && Files.size(p2) > 0) saved = true;
    } catch(e){}
  }

  // If still not saved and the object is INVALID, fall back to ALL_SOURCE for source text
  if (!saved && status === 'INVALID') {
    try {
      ctx.write('Falling back to ALL_SOURCE for ' + on + '\n');
      sqlcl.setStmt('spool "' + absFile + '"'); sqlcl.run();
      sqlcl.setStmt("select text from all_source where owner='" + TARGET + "' and name='" + on.replace("'","''") + "' order by line"); sqlcl.run();
      sqlcl.setStmt('spool off'); sqlcl.run();
      var p3 = Paths.get(absFile); if (Files.exists(p3) && Files.size(p3) > 0) saved = true;
    } catch(e) {}
  }

  if (!saved) {
    ctx.write('Warning: failed to extract DDL for ' + TARGET + '.' + on + '\n');
  } else {
    ctx.write('Saved: ' + absFile + '\n');
    count++;
  }
});

ctx.write('Export complete: ' + TARGET + ' -> ' + ROOT + ' (' + count + ' objects)\n');

