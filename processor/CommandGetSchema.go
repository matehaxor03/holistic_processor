package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"

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

	queue_name, queue_name_errors := request.GetString("[queue]")
	if queue_name_errors != nil {
		return queue_name_errors
	} else if common.IsNil(queue_name) {
		errors = append(errors, fmt.Errorf("[queue] %s is nil"))
		return errors
	}

	_, unsafe_table_name, _ := strings.Cut(*queue_name, "_")
									
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
	}

	response_queue_result.SetMapValue("data", schema)
		
	return nil
}

func commandGetSchemaFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandGetSchema
	return &funcValue
}