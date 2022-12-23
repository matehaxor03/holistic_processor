package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func commandRunNotStarted(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error

	request_keys := request.Keys()
	request_inner_map, request_inner_map_errors := request.GetMap(request_keys[0])
	if request_inner_map_errors != nil {
		errors = append(errors, request_inner_map_errors...)
	} else if common.IsNil(request_inner_map) {
		errors = append(errors, fmt.Errorf("request inner json is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 

	request_data, request_data_errors := request_inner_map.GetMap("data")
	if request_data_errors != nil {
		errors = append(errors, request_data_errors...) 
	} else if common.IsNil(request_data) {
		errors = append(errors, fmt.Errorf("request data is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 

	build_branch_instance_step_id, build_branch_instance_step_id_errors := request_data.GetUInt64("build_branch_instance_step_id")
	if build_branch_instance_step_id_errors != nil {
		errors = append(errors, build_branch_instance_step_id_errors...) 
	} else if common.IsNil(build_branch_instance_step_id) {
		errors = append(errors, fmt.Errorf("build_branch_instance_step_id is nil"))
	}

	build_branch_instance_id, build_branch_instance_id_errors := request_data.GetUInt64("build_branch_instance_id")
	if build_branch_instance_id_errors != nil {
		errors = append(errors, build_branch_instance_id_errors...) 
	} else if common.IsNil(build_branch_instance_id) {
		errors = append(errors, fmt.Errorf("build_branch_instance_id is nil"))
	}

	build_step_id, build_step_id_errors := request_data.GetUInt64("build_step_id")
	if build_step_id_errors != nil {
		errors = append(errors, build_branch_instance_id_errors...) 
	} else if common.IsNil(build_step_id) {
		errors = append(errors, fmt.Errorf("build_step_id is nil"))
	}

	order, order_errors := request_data.GetInt64("order")
	if order_errors != nil {
		errors = append(errors, order_errors...) 
	} else if common.IsNil(order) {
		errors = append(errors, fmt.Errorf("build_step_id_errors is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 

	// do something


	read_records_build_branch_instance_step_request := json.Map{"ReadRecords_BuildBranchInstanceStep":json.Map{"[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order"}, "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id, "order":*order}, "[where_fields_logic]":json.Map{"order":">"}, "[order_by]":json.Array{json.Map{"order":"ascending"}}, "[limit]":1}}
	read_records_build_branch_instance_step_response, read_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(&read_records_build_branch_instance_step_request)
	if read_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, read_records_build_branch_instance_step_response_errors...)
	} else if common.IsNil(read_records_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_build_branch_instance_step_response_keys := read_records_build_branch_instance_step_response.Keys()
	read_records_build_branch_instance_step_response_key := read_records_build_branch_instance_step_response_keys[0]
	read_records_build_branch_instance_step_response_inner, read_records_build_branch_instance_step_response_inner_errors  := (*read_records_build_branch_instance_step_response).GetMap(read_records_build_branch_instance_step_response_key)
	if read_records_build_branch_instance_step_response_inner_errors != nil {
		errors = append(errors, read_records_build_branch_instance_step_response_inner_errors...)
	} else if common.IsNil(read_records_build_branch_instance_step_response_inner) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response_inner is nil"))
	}

	if len(errors) > 0 {
		return errors
	}
	
	first_build_step_array, first_build_step_array_errors := read_records_build_branch_instance_step_response_inner.GetArray("data")
	if first_build_step_array_errors != nil {
		errors = append(errors, first_build_step_array_errors...)
	} else if common.IsNil(first_build_step_array) {
		errors = append(errors, fmt.Errorf("first_build_step_array is nil"))
	} else if len(*first_build_step_array) != 1 {
		errors = append(errors, fmt.Errorf("first_build_step_array does not have one element"))

	}

	if len(errors) > 0 {
		return errors
	}
	
	var first_build_step json.Map
	first_build_step_interface := (*first_build_step_array)[0]
	type_of_first_build_step := common.GetType(first_build_step_interface)

	if type_of_first_build_step == "json.Map" {
		first_build_step = first_build_step_interface.(json.Map)
	} else if type_of_first_build_step == "*json.Map" {
		first_build_step = *(first_build_step_interface.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("first build step has invalid type"))
	}

	if len(errors) > 0 {
		return errors
	}

	desired_build_step_id, desired_build_step_id_errors := first_build_step.GetUInt64("build_step_id")
	if desired_build_step_id_errors != nil {
		errors = append(errors, desired_build_step_id_errors...)
	} else if common.IsNil(desired_build_step_id) {
		errors = append(errors, fmt.Errorf("build_step_id is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_build_step_request := json.Map{"ReadRecords_BuildStep":json.Map{"[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"build_step_id":*desired_build_step_id}, "[select_fields]": json.Array{"name"}, "[limit]":1}}
	read_records_build_step_response, read_records_build_step_response_errors := processor.SendMessageToQueue(&read_records_build_step_request)
	if read_records_build_step_response_errors != nil {
		errors = append(errors, read_records_build_step_response_errors...)
	} else if common.IsNil(read_records_build_step_response) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_build_step_response_keys := read_records_build_step_response.Keys()
	read_records_build_step_response_key := read_records_build_step_response_keys[0]
	read_records_build_step_response_key_response_inner, read_records_build_step_response_key_response_inner_errors  := (*read_records_build_step_response).GetMap(read_records_build_step_response_key)
	if read_records_build_step_response_key_response_inner_errors != nil {
		errors = append(errors, read_records_build_step_response_key_response_inner_errors...)
	} else if common.IsNil(read_records_build_step_response_key_response_inner) {
		errors = append(errors, fmt.Errorf("read_records_build_step_response_key_response_inner is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	lookup_build_step_array, lookup_build_step_array_errors := read_records_build_step_response_key_response_inner.GetArray("data")
	if lookup_build_step_array_errors != nil {
		errors = append(errors, lookup_build_step_array_errors...)
	} else if common.IsNil(lookup_build_step_array) {
		errors = append(errors, fmt.Errorf("lookup_build_step_array is nil"))
	} else if len(*lookup_build_step_array) != 1 {
		errors = append(errors, fmt.Errorf("lookup_build_step_array does not have one element"))
	}

	if len(errors) > 0 {
		return errors
	}

	var build_step json.Map
	build_step_interface := (*lookup_build_step_array)[0]
	type_of_build_step := common.GetType(build_step_interface)

	if type_of_build_step == "json.Map" {
		build_step = build_step_interface.(json.Map)
	} else if type_of_build_step == "*json.Map" {
		build_step = *(build_step_interface.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("build step has invalid type"))
	}

	if len(errors) > 0 {
		return errors
	}

	name_of_next_step, name_of_next_step_errors := build_step.GetString("name")
	if name_of_next_step_errors != nil {
		errors = append(errors, name_of_next_step_errors...)
	} else if common.IsNil(name_of_next_step) {
		errors = append(errors, fmt.Errorf("name attribute for next step is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	next_command := json.Map{*name_of_next_step:json.Map{"data":first_build_step,"[queue_mode]":"PushBack","[async]":false, "[trace_id]":processor.GenerateTraceId()}}
	_, message_errors := processor.SendMessageToQueue(&next_command)
	if message_errors != nil {
		return message_errors
	}

	return nil
}

func commandRunNotStartedFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunNotStarted
	return &funcValue
}