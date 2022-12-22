package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
)

func commandGetTableNames(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	temp_client := processor.GetClientRead()
	
	temp_read_database, temp_read_database_errors := temp_client.GetDatabase()
	if temp_read_database_errors != nil {
		return temp_read_database_errors
	}

	table_names, table_name_errors := temp_read_database.GetTableNames()
	if table_name_errors != nil {
		return table_name_errors
	}


	array, array_errors := json.ToArray(table_names)
	if array_errors != nil {
		return array_errors
	}

	response_queue_result.SetArray("data", array)
		
	return nil
}

func commandGetTableNamesFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandGetTableNames
	return &funcValue
}