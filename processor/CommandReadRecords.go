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

	json_map_inner, json_map_inner_errors := request.GetMap(queue_name)
	if json_map_inner_errors != nil {
		return json_map_inner_errors
	} else if common.IsNil(json_map_inner) {
		errors = append(errors, fmt.Errorf("request payload %s is nil", unsafe_table_name))
		return errors
	}

	minimal_fields := false
	select_fields, select_fields_errors := json_map_inner.GetArray("[select_fields]")
	if select_fields_errors != nil {
		return select_fields_errors
	} else if !common.IsNil(select_fields) {
		for _, field := range *select_fields {
			if *(field.(*string)) == "[minimal_fields]" {
				minimal_fields = true
			}
		}
	}

	select_fields_actual := json.Array{}
	if minimal_fields {
		identity_fields, identity_fields_errors := table.GetIdentityColumns()
		if identity_fields_errors != nil {
			return identity_fields_errors
		}

		for _, identity_field := range *identity_fields {
			select_fields_actual = append(select_fields_actual, identity_field)
		}

		non_identify_fields, non_identify_fields_errors := table.GetNonIdentityColumns()
		if non_identify_fields_errors != nil {
			return non_identify_fields_errors
		}

		if common.Contains(*non_identify_fields, "name") {
			select_fields_actual = append(select_fields_actual, "name")
		}
	} 

	records, records_errors := table.ReadRecords(json.Map{}, select_fields_actual, nil, nil)
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