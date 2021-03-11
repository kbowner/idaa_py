import ibm_db, sqlparse

cDATABASE = "SAMPLE"
cHOSTNAME = "192.168.253.128"
cPORT = "50000"
cPROTOCOL = "TCPIP"
cUID = "db2inst1"
cPWD = "db2inst1"


def get_legacy_view_name(pda_schema, pda_view):
    conn = ibm_db.connect(f"DATABASE={cDATABASE};HOSTNAME={cHOSTNAME};PORT={cPORT};PROTOCOL={cPROTOCOL};UID={cUID};PWD={cPWD};", "", "")
    sql = f"SELECT OLD_SCHEMA, OLD_VIEW_NAME FROM IDAA.PDA_VIEW_MAP WHERE NEW_SCHEMA = '{pda_schema}' AND NEW_VIEW_NAME = '{pda_view}' FETCH FIRST 1 ROW ONLY;"
    stmt = ibm_db.exec_immediate(conn, sql)
    tuple = ibm_db.fetch_tuple(stmt)
    while tuple:
        return tuple[0], tuple[1]
        tuple = ibm_db.fetch_tuple(stmt)

def get_idaa_view_header(pda_schema, pda_view):
    conn = ibm_db.connect(f"DATABASE={cDATABASE};HOSTNAME={cHOSTNAME};PORT={cPORT};PROTOCOL={cPROTOCOL};UID={cUID};PWD={cPWD};", "", "")
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
    print(header_begin)
    for items in col_list:
        print(items)
    print(header_end)

def get_bmsiw_view_body(schema, view):
    conn = ibm_db.connect(f"DATABASE={cDATABASE};HOSTNAME={cHOSTNAME};PORT={cPORT};PROTOCOL={cPROTOCOL};UID={cUID};PWD={cPWD};", "", "")
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


if __name__ == '__main__':
    nz_schema = 'LEDGER'
    nz_view = 'LEDGER_2020_V'
    bmsiw_schema, bmsiw_view = get_legacy_view_name(nz_schema, nz_view)
    print("--", "*" * 80)
    print(f"-- * PDA view: {nz_schema:>23}.{nz_view}")
    print(f"-- * Legacy view: {bmsiw_schema:>20}.{bmsiw_view}")
    print("--","*"*80)
    print_idaa_view_header(nz_schema, nz_view)
    bmsiw_stmt = get_bmsiw_view_body(bmsiw_schema, bmsiw_view)
    #print(bmsiw_stmt)
    # Print a formatted view's DDL code using sqlparse module
    print(sqlparse.format(bmsiw_stmt, reindent=True))
