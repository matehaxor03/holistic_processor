package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
	"strings"
)

func commandGetTableCount(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
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

	count, count_errors := table.Count(where_fields_actual, where_fields_logic_actual, group_by_actual, order_by_actual, limit_actual, offset_actual)
	if count_errors != nil {
		return count_errors
	} else if common.IsNil(count) {
		errors = append(errors, fmt.Errorf("count for table %s is nil", unsafe_table_name))
		return errors
	}

	if len(errors) > 0 {
		return errors
	} 
	
	response_queue_result.SetUInt64("data", count)	
	
	if include_schema_actual {
		response_queue_result.SetMap("schema", table_schema_actual)
	}	

	return nil
}

func commandGetTableCountFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandGetTableCount
	return &funcValue
}