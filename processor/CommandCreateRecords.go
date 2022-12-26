package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
	"strings"
)

func commandCreateRecords(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error
	temp_write_client := processor.GetClientWrite()
	
	temp_write_database, temp_write_database_errors := temp_write_client.GetDatabase()
	if temp_write_database_errors != nil {
		return temp_write_database_errors
	}

	queue_name, queue_name_errors := request.GetString("[queue]")
	if queue_name_errors != nil {
		return queue_name_errors
	} else if common.IsNil(queue_name) {
		errors = append(errors, fmt.Errorf("[queue] %s is nil"))
		return errors
	}

	_, unsafe_table_name, _ := strings.Cut(*queue_name, "_")						
	table, table_errors := temp_write_database.GetTable(unsafe_table_name)
	if table_errors != nil {
		return table_errors
	} else if common.IsNil(table) {
		errors = append(errors, fmt.Errorf("table %s is nil", unsafe_table_name))
		return errors
	}

	data_array, data_array_errors := request.GetArray("data")
	if data_array_errors != nil {
		return data_array_errors
	} else if common.IsNil(data_array) {
		errors = append(errors, fmt.Errorf("request data %s is nil", unsafe_table_name))
		return errors
	}

	update_records_errors := table.CreateRecords(*data_array)
	if update_records_errors != nil {
		return update_records_errors
	}

	if len(errors) > 0 {
		return errors
	} 

	return nil
}

func commandCreateRecordsFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandCreateRecords
	return &funcValue
}