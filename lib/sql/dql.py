QUERY_RETRIEVE_LATEST_SALES = "SELECT * FROM sales WHERE Id > %(Id)d"

QUERY_RETRIEVE_LAST_ID_BUSINESS_TABLE = "SELECT Id FROM sales ORDER BY Id DESC LIMIT 1"

QUERY_RETRIEVE_LAST_ID_CONTROL_DATA_FLOW_TABLE = "SELECT Id FROM control_data_flow WHERE table = 'sales' ORDER BY Id DESC LIMIT 1"