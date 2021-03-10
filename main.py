import ibm_db


def get_legacy_view_name(pda_schema, pda_view):
    conn = ibm_db.connect("DATABASE=SAMPLE;HOSTNAME=192.168.253.128;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=db2inst1;", "", "")
    sql = f"SELECT OLD_SCHEMA, OLD_VIEW_NAME FROM IDAA.PDA_VIEW_MAP WHERE NEW_SCHEMA = '{pda_schema}' AND NEW_VIEW_NAME = '{pda_view}' FETCH FIRST 1 ROW ONLY;"
    stmt = ibm_db.exec_immediate(conn, sql)
    tuple = ibm_db.fetch_tuple(stmt)
    while tuple != False:
        return tuple[0], tuple[1]
        tuple = ibm_db.fetch_tuple(stmt)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    nz_schema = 'LEDGER'
    nz_view = 'LEDGER_2020_V'
    bmsiw_schema, bmsiw_view = get_legacy_view_name(nz_schema, nz_view)
    print(f"PDA2 view: {nz_schema:>22}.{nz_view}")
    print(f"Legacy view: {bmsiw_schema:>20}.{bmsiw_view}")
