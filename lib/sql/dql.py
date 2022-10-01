QUERY_RETRIEVE_LATEST_SALES = "SELECT id, product, price, CAST(dateevent as VARCHAR) as dateevent, CAST(createddate as VARCHAR) as createddate FROM dw.sales WHERE Id > %(id)s"

QUERY_RETRIEVE_LAST_ID_BUSINESS_TABLE = "SELECT id FROM dw.sales ORDER BY id DESC LIMIT 1"

QUERY_RETRIEVE_LAST_ID_CONTROL_DATA_FLOW_TABLE = "SELECT id FROM dw.control_data_flow WHERE schema_name = 'dw' and table_name = 'sales' ORDER BY id DESC LIMIT 1;"