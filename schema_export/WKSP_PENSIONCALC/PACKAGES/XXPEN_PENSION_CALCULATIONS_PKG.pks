SQL> DECLARE
  2    l_ddl CLOB;
  3    l_pos INTEGER := 1;
  4    l_len INTEGER;
  5    l_chunk VARCHAR2(32767);
  6  BEGIN
  7    BEGIN
  8      l_ddl := DBMS_METADATA.GET_DDL('PACKAGE','XXPEN_PENSION_CALCULATIONS_PKG','WKSP_PENSIONCALC');
  9      IF l_ddl IS NOT NULL THEN
 10        l_len := DBMS_LOB.GETLENGTH(l_ddl);
 11        WHILE l_pos <= l_len LOOP
 12          l_chunk := DBMS_LOB.SUBSTR(l_ddl,32767,l_pos);
 13          DBMS_OUTPUT.PUT_LINE(l_chunk);
 14          l_pos := l_pos + 32767;
 15        END LOOP;
 16      END IF;
 17    EXCEPTION WHEN OTHERS THEN
 18      DBMS_OUTPUT.PUT_LINE('--ERROR:'||SQLERRM);
 19      FOR r IN (SELECT text FROM all_source WHERE owner='WKSP_PENSIONCALC' AND name='XXPEN_PENSION_CALCULATIONS_PKG' ORDER BY line) LOOP
 20        DBMS_OUTPUT.PUT_LINE(r.text);
 21      END LOOP;
 22    END;
 23  END;
 24  /
--ERROR:ORA-31603: object "XXPEN_PENSION_CALCULATIONS_PKG" of type PACKAGE not found in schema "WKSP_PENSIONCALC"


PL/SQL procedure successfully completed.

SQL> spool off
