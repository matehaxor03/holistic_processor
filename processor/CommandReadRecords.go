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

	where_fields_actual := json.NewArray()
	where_fields, where_fields_errors := request.GetArray("[where_fields]")
	if where_fields_errors != nil {
		return where_fields_errors
	} else if !common.IsNil(where_fields) {
		where_fields_actual = where_fields
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
	} else if !common.IsNil(select_fields) {
		select_fields_actual = select_fields
	} 

	group_by_actual := json.NewArray()
	group_by_fields, group_by_fields_errors := request.GetArray("[group_by]")
	if group_by_fields_errors != nil {
		return group_by_fields_errors
	} else if !common.IsNil(group_by_fields) {
		group_by_actual = group_by_fields
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

	var offset_actual *uint64
	offset_value, offset_value_errors := request.GetUInt64("[offset]")
	if offset_value_errors != nil {
		return offset_value_errors
	} else if !common.IsNil(offset_value) {
		offset_actual = offset_value
	}

	records, records_errors := table.ReadRecords(select_fields_actual, where_fields_actual, group_by_actual, order_by_actual, limit_actual, offset_actual)
	if records_errors != nil {
		return records_errors
	} else if common.IsNil(records) {
		errors = append(errors, fmt.Errorf("records for table %s is nil", unsafe_table_name))
		return errors
	}

	include_count_actual := false
	include_count, include_count_errors := request.GetBool("[include_count]")
	if include_count_errors != nil {
		return include_count_errors
	} else if !common.IsNil(include_count) {
		include_count_actual = *include_count
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

	if include_count_actual {
		count, count_errors := table.Count(where_fields_actual, group_by_actual, nil, nil, nil)
		if count_errors != nil {
			return count_errors
		} else if common.IsNil(count) {
			errors = append(errors, fmt.Errorf("count for table %s is nil", unsafe_table_name))
			return errors
		} else {
			response_queue_result.SetUInt64("count", count)
		}
	}

	return nil
}

func commandReadRecordsFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandReadRecords
	return &funcValue
}