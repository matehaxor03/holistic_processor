package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
	"strings"
)

func commandCreateRecord(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error
	temp_write_client := processor.GetClientWrite()
	
	temp_write_database := temp_write_client.GetDatabase()
	if temp_write_database == nil {
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
									
	table, table_errors := temp_write_database.GetTable(unsafe_table_name)
	if table_errors != nil {
		return table_errors
	} else if common.IsNil(table) {
		errors = append(errors, fmt.Errorf("table %s is nil", unsafe_table_name))
		return errors
	}

	data_map, data_map_errors := request.GetMap("data")
	if data_map_errors != nil {
		return data_map_errors
	} else if common.IsNil(data_map) {
		errors = append(errors, fmt.Errorf("request data %s is nil", unsafe_table_name))
		return errors
	}

	async, async_errors := request.GetBool("[async]")
	if async_errors != nil {
		return async_errors
	} else if common.IsNil(async) {
		temp_async := false
		async = &temp_async
		request.SetBool("[async]", async)
	}
	
	if !*async {
		new_record, create_record_errors := table.CreateRecord(*data_map)
		if create_record_errors != nil {
			return create_record_errors
		} else if common.IsNil(new_record) {
			errors = append(errors, fmt.Errorf("created record %s is nil", unsafe_table_name))
			return errors
		} else {
			new_record_fields, new_record_fields_errors := new_record.GetFields()
			if new_record_fields_errors != nil {
				return new_record_fields_errors
			} else {
				response_queue_result.SetMap("data", new_record_fields)	

				if *queue_name == "CreateRecord_BuildBranchInstance" {
					callback_payload_map := map[string]interface{}{"[queue]":"Run_StartBuildBranchInstance", "data":new_record_fields,"[queue_mode]":"PushBack","[async]":true, "[trace_id]":processor.GenerateTraceId()}
					go processor.SendMessageToQueueFireAndForget(json.NewMapOfValues(&callback_payload_map))
				}
			}
		}
	} else {
		response_queue_result.SetMap("data", nil)	
		create_record_errors := table.CreateRecordAsync(*data_map)
		if create_record_errors != nil {
			errors = append(errors, create_record_errors...)
			return errors
		} 
	}
	

	if len(errors) > 0 {
		return errors
	} 

	return nil
}

func commandCreateRecordFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandCreateRecord
	return &funcValue
}