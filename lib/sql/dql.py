QUERY_RETRIEVE_LATEST_SALES = "SELECT * FROM dw.sales WHERE Id > %(Id)d"

QUERY_RETRIEVE_LAST_ID_BUSINESS_TABLE = "SELECT Id FROM dw.sales ORDER BY Id DESC LIMIT 1"

QUERY_RETRIEVE_LAST_ID_CONTROL_DATA_FLOW_TABLE = "SELECT Id FROM dw.control_data_flow WHERE schema_name = 'dw' and table_name = 'sales' ORDER BY Id DESC LIMIT 1"