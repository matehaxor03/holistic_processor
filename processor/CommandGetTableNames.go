package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	"fmt"
)

func commandGetTableNames(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error
	temp_client := processor.GetClientRead()
	
	temp_read_database := temp_client.GetDatabase()
	if temp_read_database == nil {
		errors = append(errors, fmt.Errorf("database is nil"))
		return errors
	}

	table_names, table_names_errors := temp_read_database.GetTableNames()
	if table_names_errors != nil {
		return table_names_errors
	}

	table_names_array := json.NewArray()
	for _, table_name := range table_names {
		table_names_array.AppendStringValue(table_name)
	}

	response_queue_result.SetArray("data", table_names_array)
	return nil
}

func commandGetTableNamesFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandGetTableNames
	return &funcValue
}