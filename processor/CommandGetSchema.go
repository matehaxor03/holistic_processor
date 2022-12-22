package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	"fmt"
	"strings"
)

func commandGetSchema(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error
	temp_client := processor.GetClientRead()
	
	temp_read_database, temp_read_database_errors := temp_client.GetDatabase()
	if temp_read_database_errors != nil {
		return temp_read_database_errors
	}

	queue_name := (request.Keys())[0]
	_, unsafe_table_name, _ := strings.Cut(queue_name, "_")
									
	table, table_errors := temp_read_database.GetTable(unsafe_table_name)
	if table_errors != nil {
		return table_errors
	} else if table == nil {
		errors = append(errors, fmt.Errorf("table %s is nil", unsafe_table_name))
		return errors
	}

	schema, schema_errors := table.GetSchema()
	if schema_errors != nil {
		return schema_errors
	} else if schema == nil {
		errors = append(errors, fmt.Errorf("schema %s is nil", unsafe_table_name))
		return errors
	}

	response_queue_result.SetMap("data", schema)
		
	return nil
}

func commandGetSchemaFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandGetSchema
	return &funcValue
}