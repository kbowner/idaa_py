import ibm_db, sqlparse
from colorama import Fore
from colorama import Style

cDATABASE = "SAMPLE"
cHOSTNAME = "192.168.253.128"
cPORT = "50000"
cPROTOCOL = "TCPIP"
cUID = "db2inst1"
cPWD = "db2inst1"
standard_audit_col_list = ['ROW_STAT_CD', 'INSRT_TMS', 'UPDT_TMS']

conn = ibm_db.connect(f"DATABASE={cDATABASE};HOSTNAME={cHOSTNAME};PORT={cPORT};PROTOCOL={cPROTOCOL};UID={cUID};PWD={cPWD};", "", "")


def get_legacy_view_name(pda_schema, pda_view):
    sql = f"SELECT OLD_SCHEMA, OLD_VIEW_NAME FROM IDAA.PDA_VIEW_MAP WHERE NEW_SCHEMA = '{pda_schema}' AND NEW_VIEW_NAME = '{pda_view}' FETCH FIRST 1 ROW ONLY;"
    stmt = ibm_db.exec_immediate(conn, sql)
    tuple = ibm_db.fetch_tuple(stmt)
    while tuple:
        return tuple[0], tuple[1]
        tuple = ibm_db.fetch_tuple(stmt)

def get_idaa_view_header(pda_schema, pda_view):
    sql = f"SELECT COLNAME||',' FROM IDAA.PDA_REL_COLUMNS WHERE CREATOR = '{pda_schema}' AND NAME = '{pda_view}' ORDER BY COLNO;"
    stmt = ibm_db.exec_immediate(conn, sql)
    s2 = ibm_db.fetch_tuple(stmt)
    cols = []
    while s2:
        cols.append (s2[0])
        s2 = ibm_db.fetch_tuple(stmt)
    s1 = "CREATE VIEW " + pda_schema + "." + pda_view + "("
    col_list_len = len(cols)
    col_list = []
    for i in range (0,col_list_len-1):
        col_list.append(cols[i])
    last_col = col_list[i]
    col_list.append(last_col[:-1])
    s3 = ")"
    return s1, col_list, s3

def print_idaa_view_header(schema, view):
    col_list = []
    header_begin, col_list, header_end = get_idaa_view_header(schema, view)
    s = ""
    for items in col_list:
        s = s + " " + items
    idaa_header = header_begin+ " " + s + " " + header_end
    return idaa_header

def get_bmsiw_view_body(schema, view):
    sql = f"SELECT STMT, LENGTH(STMT) AS LENS FROM IDAA.BMSIW_VIEWS WHERE SCHEMA = '{schema}' AND VIEWNAME = '{view}';"
    stmt = ibm_db.exec_immediate(conn, sql)
    stmt_len = 0
    stmt_txt = ""
    res = ibm_db.fetch_tuple(stmt)
    while res:
        stmt_len = res[1]
        stmt_txt = res[0]
        res = ibm_db.fetch_tuple(stmt)
    view_body = stmt_txt[stmt_txt.find('AS'):]+';'
    return view_body

def get_view_row_count(src_db, schema, view):
    #src_db - either PDA or DB2
    if src_db == 'DB2':
        sql = f"SELECT COUNT(*) AS CNT FROM IDAA.BMSIW_COLUMNS WHERE SCHEMA = '{schema}' AND VIEWNAME = '{view}';"
    elif src_db == 'PDA':
        sql = f"SELECT COUNT(*) AS CNT FROM IDAA.PDA_REL_COLUMNS WHERE CREATOR = '{schema}' AND NAME = '{view}';"
    else:
        print('ERROR: Data not found!')
    stmt = ibm_db.exec_immediate(conn, sql)
    res = ibm_db.fetch_tuple(stmt)
    while res:
        return res[0]
        res = ibm_db.fetch_tuple(stmt)

def check_audit_col (src_db, schema, view):
    # check if there are audit columns in PDA and BMSIW views
    # src_db - either PDA or DB2
    if src_db == 'DB2':
        sql = f"SELECT COLNAME, COLNO FROM IDAA.BMSIW_COLUMNS WHERE SCHEMA = '{schema}' AND "\
                f"VIEWNAME = '{view}' ORDER BY COLNO DESC FETCH FIRST 3 ROWS ONLY;"
    elif src_db == 'PDA':
        sql = f"SELECT COLNAME, COLNO FROM IDAA.PDA_REL_COLUMNS WHERE CREATOR = '{schema}' AND "\
                f"NAME = '{view}' ORDER BY COLNO DESC FETCH FIRST 3 ROWS ONLY;"
    else:
        print('ERROR: Data not found!')
    stmt = ibm_db.exec_immediate(conn, sql)
    res = ibm_db.fetch_tuple(stmt)
    aud_col_list = []
    while res:
        aud_col_list.append(res[0])
        res = ibm_db.fetch_tuple(stmt)
    aud_col_list.reverse()
    return aud_col_list

def compare_audit_col(db2_list, pda_list):
    # Now check the order of the audit columns
    if (all(x in db2_list for x in pda_list)) and (db2_list == standard_audit_col_list) and (db2_list == pda_list):
        print(f"{Fore.LIGHTGREEN_EX}Audit columns: YES (PDA and DB2); Last 3 columns and order are the same.{Style.RESET_ALL}")
    elif (all(x in db2_list for x in pda_list)) and (db2_list == standard_audit_col_list) and (db2_list != pda_list):
        print(f"{Fore.GREEN}Audit columns: YES (PDA and DB2); Last 3 columns are the same, but the order is different.{Style.RESET_ALL}")
    elif (db2_list != standard_audit_col_list) and (pda_list != standard_audit_col_list) \
            and (get_view_row_count('DB2', bmsiw_schema, bmsiw_view) == get_view_row_count('PDA', nz_schema, nz_view)):
        print(f"{Fore.GREEN}Audit columns: NO (PDA and DB2); Both views column count is equal.{Style.RESET_ALL}")
    elif (db2_list != standard_audit_col_list) and (pda_list != standard_audit_col_list) \
            and (get_view_row_count('DB2', bmsiw_schema, bmsiw_view) != get_view_row_count('PDA', nz_schema, nz_view)):
        print(f"{Fore.RED}Audit columns: NO (PDA and DB2); Both views column count is different!{Style.RESET_ALL}")
    elif (db2_list == standard_audit_col_list) and (pda_list != standard_audit_col_list) \
            and (get_view_row_count('DB2', bmsiw_schema, bmsiw_view) != get_view_row_count('PDA', nz_schema, nz_view)):
        print(f"{Fore.RED}Audit columns: YES (DB2) and NO (PDA); Both views column count is different!{Style.RESET_ALL}")
    elif (db2_list != standard_audit_col_list) and (pda_list == standard_audit_col_list) \
            and (get_view_row_count('DB2', bmsiw_schema, bmsiw_view) != get_view_row_count('PDA', nz_schema, nz_view)):
        print(f"{Fore.RED}Audit columns: NO (DB2) and YES (PDA); Both views column count is different!{Style.RESET_ALL}")
    elif (db2_list == standard_audit_col_list) and (pda_list != standard_audit_col_list) \
            and (get_view_row_count('DB2', bmsiw_schema, bmsiw_view) == get_view_row_count('PDA', nz_schema, nz_view)):
        print(f"{Fore.RED}Audit columns: YES (DB2) and NO (PDA); But views column count is the same!{Style.RESET_ALL}")
    elif (db2_list != standard_audit_col_list) and (pda_list == standard_audit_col_list) \
            and (get_view_row_count('DB2', bmsiw_schema, bmsiw_view) == get_view_row_count('PDA', nz_schema, nz_view)):
        print(f"{Fore.RED}Audit columns: NO (DB2) and YES (PDA); But views column count is the same!{Style.RESET_ALL}")
    else:
        print(f"{Fore.RED}Check columns and fix header/body of the view!{Style.RESET_ALL}")

    return 0


if __name__ == '__main__':
    nz_schema = 'LEDGER'
    nz_view = 'LEDGER_2020_V'
    bmsiw_schema, bmsiw_view = get_legacy_view_name(nz_schema, nz_view)
    print("--", "#" * 80)
    print(f"-- # PDA view: {nz_schema:>23}.{nz_view}")
    print(f"-- # Legacy view: {bmsiw_schema:>20}.{bmsiw_view}")
    print("--", "#")
    print("--", f"# Legacy view column count = {get_view_row_count('DB2', bmsiw_schema, bmsiw_view):>5}")
    print("--", f"# PDA view column count = {get_view_row_count('PDA', nz_schema, nz_view):>8}")
    print("--","#"*80)
    idaa_view_header = print_idaa_view_header(nz_schema, nz_view)
    idaa_view_statement = get_bmsiw_view_body(bmsiw_schema, bmsiw_view)
    # Print a formatted view's DDL code using sqlparse module
    print(sqlparse.format(idaa_view_header, reindent=True, wrap_after=True, truncate_strings=80))
    print(sqlparse.format(idaa_view_statement, reindent=True, wrap_after=True, truncate_strings=80))
    print(check_audit_col ('DB2', bmsiw_schema, bmsiw_view))
    print(check_audit_col ('PDA', nz_schema, nz_view))
    compare_audit_col(check_audit_col ('DB2', bmsiw_schema, bmsiw_view),check_audit_col ('PDA', nz_schema, nz_view))

