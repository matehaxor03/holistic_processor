package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
	"strings"
)

func commandReadRecords(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error
	temp_client := processor.GetClientRead()
	
	temp_read_database, temp_read_database_errors := temp_client.GetDatabase()
	if temp_read_database_errors != nil {
		return temp_read_database_errors
	}

	keys := request.Keys()
	queue_name := keys[0]
	_, unsafe_table_name, _ := strings.Cut(queue_name, "_")
	fmt.Println(unsafe_table_name)
									
	table, table_errors := temp_read_database.GetTable(unsafe_table_name)
	if table_errors != nil {
		return table_errors
	} else if common.IsNil(table) {
		errors = append(errors, fmt.Errorf("table %s is nil", unsafe_table_name))
		return errors
	}

	records, records_errors := table.ReadRecords(json.Map{}, nil, nil)
	if records_errors != nil {
		return records_errors
	} else if common.IsNil(records) {
		errors = append(errors, fmt.Errorf("records for table %s is nil", unsafe_table_name))
		return errors
	}
	
	array := json.Array{}
	for _, record := range *records {
		fields_for_record, fields_for_record_error := record.GetFields()
		if fields_for_record_error != nil {
			errors = append(errors, fields_for_record_error...)
		} else {
			array = append(array, *fields_for_record)
		}		
	}

	if len(errors) > 0 {
		return errors
	} 
	
	response_queue_result.SetArray("data", &array)		
	return nil
}