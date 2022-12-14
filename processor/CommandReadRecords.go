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
	
	temp_read_database := temp_client.GetDatabase()
	if temp_read_database == nil {
		errors = append(errors, fmt.Errorf("database is nil"))
		return errors
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
	} else if common.IsNil(table) {
		errors = append(errors, fmt.Errorf("table %s is nil", unsafe_table_name))
		return errors
	}

	minimal_fields := false
	select_fields, select_fields_errors := request.GetArray("[select_fields]")
	if select_fields_errors != nil {
		return select_fields_errors
	} else if !common.IsNil(select_fields) {
		for _, field := range *(select_fields.GetValues()){
			if field.IsValueEqualToStringValue("[minimal_fields]") {
				minimal_fields = true
			}
		}
	}

	where_fields_actual := json.NewMap()
	where_fields, where_fields_errors := request.GetMap("[where_fields]")
	if where_fields_errors != nil {
		return where_fields_errors
	} else if !common.IsNil(where_fields) {
		where_fields_actual = where_fields
	}

	where_fields_logic_actual := json.NewMap()
	where_fields_logic, where_fields_logic_errors := request.GetMap("[where_fields_logic]")
	if where_fields_logic_errors != nil {
		return where_fields_logic_errors
	} else if !common.IsNil(where_fields_logic) {
		where_fields_logic_actual = where_fields_logic
	}

	include_schema_actual := false
	include_schema, include_schema_errors :=  request.GetBool("[include_schema]")
	if include_schema_errors != nil {
		return include_schema_errors
	} else if !common.IsNil(include_schema) {
		include_schema_actual = *include_schema
	}

	table_schema_actual := json.NewMap()
	if include_schema_actual {
		table_schema, table_schema_errors := table.GetSchema()
		if table_schema_errors != nil {
			return table_schema_errors
		} else {
			table_schema_actual = table_schema
		}
	}

	select_fields_actual := json.NewArray()
	if minimal_fields {
		identity_fields, identity_fields_errors := table.GetIdentityColumns()
		if identity_fields_errors != nil {
			return identity_fields_errors
		}

		for identity_field, _  := range *identity_fields {
			select_fields_actual.AppendStringValue(identity_field)
		}

		non_identify_fields, non_identify_fields_errors := table.GetNonPrimaryKeyColumns()
		if non_identify_fields_errors != nil {
			return non_identify_fields_errors
		}

		if _, found := (*non_identify_fields)[ "name"]; found {
			select_fields_actual.AppendStringValue("name")
		}
	} 

	order_by_actual := json.NewArray()
	order_by_fields, order_by_fields_errors := request.GetArray("[order_by]")
	if order_by_fields_errors != nil {
		return order_by_fields_errors
	} else if !common.IsNil(order_by_fields) {
		order_by_actual = order_by_fields
	}

	var limit_actual *uint64
	limit_value, limit_value_errors := request.GetUInt64("[limit]")
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
	
	array := json.NewArray()
	for index, record := range *records {
		fields_for_record, fields_for_record_error := record.GetFields()
		if fields_for_record_error != nil {
			errors = append(errors, fields_for_record_error...)
		} else if common.IsNil(fields_for_record){
			errors = append(errors, fmt.Errorf("record is nil for record %d", index))
		} else {
			array.AppendMap(fields_for_record)
		}	
	}

	if len(errors) > 0 {
		return errors
	} 
	
	response_queue_result.SetArray("data", array)	
	
	if include_schema_actual {
		response_queue_result.SetMap("schema", table_schema_actual)
	}	

	return nil
}

func commandReadRecordsFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandReadRecords
	return &funcValue
}