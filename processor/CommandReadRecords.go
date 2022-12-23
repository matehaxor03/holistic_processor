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

	where_fields_actual := json.Map{}
	where_fields, where_fields_errors := json_map_inner.GetMap("[where_fields]")
	if where_fields_errors != nil {
		return where_fields_errors
	} else if !common.IsNil(where_fields) {
		where_fields_actual = *where_fields
	}

	where_fields_logic_actual := json.Map{}
	where_fields_logic, where_fields_logic_errors := json_map_inner.GetMap("[where_fields_logic]")
	if where_fields_logic_errors != nil {
		return where_fields_logic_errors
	} else if !common.IsNil(where_fields_logic) {
		where_fields_logic_actual = *where_fields_logic
	}

	include_schema_actual := false
	include_schema, include_schema_errors :=  json_map_inner.GetBool("[include_schema]")
	if include_schema_errors != nil {
		return include_schema_errors
	} else if !common.IsNil(include_schema) {
		include_schema_actual = *include_schema
	}

	table_schema_actual := json.Map{}
	if include_schema_actual {
		table_schema, table_schema_errors := table.GetSchema()
		if table_schema_errors != nil {
			return table_schema_errors
		} else if common.IsNil(table_schema) {
			errors = append(errors, fmt.Errorf("table schema %s is nil", unsafe_table_name))
			return errors
		} else {
			table_schema_actual = *table_schema
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

	order_by_actual := json.Array{}
	order_by_fields, order_by_fields_errors := json_map_inner.GetArray("[order_by]")
	if order_by_fields_errors != nil {
		return order_by_fields_errors
	} else if !common.IsNil(order_by_fields) {
		order_by_actual = *order_by_fields
	}

	var limit_actual *uint64
	limit_value, limit_value_errors := json_map_inner.GetUInt64("[limit]")
	if limit_value_errors != nil {
		return limit_value_errors
	} else if !common.IsNil(limit_value) {
		limit_actual = limit_value
	}

	records, records_errors := table.ReadRecords(select_fields_actual, where_fields_actual, where_fields_logic_actual, order_by_actual, limit_actual, nil)
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
	
	if include_schema_actual {
		response_queue_result.SetMap("schema", &table_schema_actual)
	}	

	return nil
}

func commandReadRecordsFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandReadRecords
	return &funcValue
}