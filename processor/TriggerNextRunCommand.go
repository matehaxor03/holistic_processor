package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func triggerNextRunCommand(processor *Processor, command_name *string, build_branch_id *uint64, build_branch_instance_step_id *uint64, build_branch_instance_id *uint64, build_step_id  *uint64, order  *int64, domain_name *string, repository_account_name *string, repository_name *string, branch_name *string, parameters *string, errors []error, request *json.Map) ([]error) {
	if command_name == nil {
		errors = append(errors, fmt.Errorf("current command_name is nil"))
	} 

	lookup_name := ""
	if len(errors) > 0 {
		lookup_name = "Failed"
	} else {
		lookup_name = "Passed"
	}

	build_step_status_select := []string{"build_step_status_id"}
	build_step_status_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&build_step_status_select))

	build_step_status_where := map[string]interface{}{"name":lookup_name}
	build_step_status_where_map := json.NewMapOfValues(&build_step_status_where)

	build_step_status_where_request := map[string]interface{}{"[queue]":"ReadRecords_BuildStepStatus", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	build_step_status_where_request_map := json.NewMapOfValues(&build_step_status_where_request)
	build_step_status_where_request_map.SetArray("[select_fields]", build_step_status_select_array)
	build_step_status_where_request_map.SetMap("[where_fields]", build_step_status_where_map)

	//read_records_build_step_status_request := json.Map{"[queue]":"ReadRecords_BuildStepStatus", "[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"name":lookup_name}, "[select_fields]": json.Array{"build_step_status_id"}, "[limit]":1}
	read_records_build_step_status_response, read_records_build_step_status_response_errors := processor.SendMessageToQueue(build_step_status_where_request_map)
	if read_records_build_step_status_response_errors != nil {
		errors = append(errors, read_records_build_step_status_response_errors...)
	} else if common.IsNil(read_records_build_step_status_response) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_build_step_status_array, read_records_build_step_status_array_errors := read_records_build_step_status_response.GetArray("data")
	if read_records_build_step_status_array_errors != nil {
		errors = append(errors, read_records_build_step_status_array_errors...)
	} else if common.IsNil(read_records_build_step_status_array) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_array is nil"))
	} else if len(*(read_records_build_step_status_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_array did not return 1 record"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_step_status, build_step_status_errors :=  (*(read_records_build_step_status_array.GetValues()))[0].GetMap()
	if build_step_status_errors != nil {
		errors = append(errors, build_step_status_errors...)
	} else if common.IsNil(build_step_status) {
		errors = append(errors, fmt.Errorf("build_step_status is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_step_status_id, build_step_status_id_errors := build_step_status.GetUInt64("build_step_status_id")
	if build_step_status_id_errors != nil {
		errors = append(errors, build_step_status_id_errors...)
	} else if common.IsNil(build_step_status_id) {
		errors = append(errors, fmt.Errorf("build_step_status_id is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	update_records_build_branch_instance_step_data_map :=  map[string]interface{}{"build_branch_instance_step_id":build_branch_instance_step_id, "build_step_status_id":*build_step_status_id}
	update_records_build_branch_instance_step_data :=  json.NewMapOfValues(&update_records_build_branch_instance_step_data_map)
	update_records_build_branch_instance_step_array_data := json.NewArray()
	update_records_build_branch_instance_step_array_data.AppendMap(update_records_build_branch_instance_step_data)

	update_records_build_branch_instance_step_payload_map := map[string]interface{}{"[queue]":"UpdateRecords_BuildBranchInstanceStep","[queue_mode]":"PushBack","[trace_id]":processor.GenerateTraceId()}
	update_records_build_branch_instance_step_payload := json.NewMapOfValues(&update_records_build_branch_instance_step_payload_map)
	update_records_build_branch_instance_step_payload.SetArray("data", update_records_build_branch_instance_step_array_data)
	
	//update_records_build_branch_instance_step_request := json.Map{"[queue]":"UpdateRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "data": json.Array{json.Map{"build_branch_instance_step_id":build_branch_instance_step_id, "build_step_status_id":*build_step_status_id}}}
	_, update_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(update_records_build_branch_instance_step_payload)
	if update_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, update_records_build_branch_instance_step_response_errors...)
	}

	if len(errors) > 0 {
		return errors
	}
	
	build_branch_instance_step_select := []string{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order"}
	build_branch_instance_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&build_branch_instance_step_select))

	build_branch_instance_step_where := map[string]interface{}{"build_branch_instance_id":*build_branch_instance_id, "order":*order}
	build_branch_instance_step_where_map := json.NewMapOfValues(&build_branch_instance_step_where)

	build_branch_instance_step_where_logic := map[string]interface{}{"order":">"}
	build_branch_instance_step_where_logic_map := json.NewMapOfValues(&build_branch_instance_step_where_logic)

	build_branch_instance_step_order_by := map[string]interface{}{"order":"ascending"}
	build_branch_instance_step_order_by_map := json.NewMapOfValues(&build_branch_instance_step_order_by)
	order_by_array := json.NewArray()
	order_by_array.AppendMap(build_branch_instance_step_order_by_map)

	build_branch_instance_step_where_request := map[string]interface{}{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	build_branch_instance_step_where_request_map := json.NewMapOfValues(&build_branch_instance_step_where_request)
	build_branch_instance_step_where_request_map.SetArray("[select_fields]", build_branch_instance_step_select_array)
	build_branch_instance_step_where_request_map.SetMap("[where_fields]", build_branch_instance_step_where_map)
	build_branch_instance_step_where_request_map.SetMap("[where_fields_logic]", build_branch_instance_step_where_logic_map)
	build_branch_instance_step_where_request_map.SetArray("[order_by]", order_by_array)


	//read_records_build_branch_instance_step_request := json.Map{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order"}, "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id, "order":*order}, "[where_fields_logic]":json.Map{"order":">"}, "[order_by]":json.Array{json.Map{"order":"ascending"}}, "[limit]":1}
	read_records_build_branch_instance_step_response, read_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(build_branch_instance_step_where_request_map)
	if read_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, read_records_build_branch_instance_step_response_errors...)
	} else if common.IsNil(read_records_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}
	
	next_branch_instance_steps, next_branch_instance_steps_errors := read_records_build_branch_instance_step_response.GetArray("data")
	if next_branch_instance_steps_errors != nil {
		errors = append(errors, next_branch_instance_steps_errors...)
	} else if common.IsNil(next_branch_instance_steps) {
		errors = append(errors, fmt.Errorf("next_branch_instance_steps is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	if len(*(next_branch_instance_steps.GetValues())) == 0 {
		return nil
	}

	next_branch_instance_build_step, next_branch_instance_build_step_errors :=  (*(next_branch_instance_steps.GetValues()))[0].GetMap()
	if next_branch_instance_build_step_errors != nil {
		errors = append(errors, next_branch_instance_build_step_errors...)
	} else if common.IsNil(next_branch_instance_build_step) {
		errors = append(errors, fmt.Errorf("next_branch_instance_build_step is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	new_order, new_order_errors := next_branch_instance_build_step.GetInt64("order")
	if new_order_errors != nil {
		errors = append(errors, new_order_errors...)
	} else if common.IsNil(new_order) {
		errors = append(errors, fmt.Errorf("new_order is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	next_build_branch_instance_step_select := []string{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order", "parameters"}
	next_build_branch_instance_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&next_build_branch_instance_step_select))

	next_build_branch_instance_step_where := map[string]interface{}{"build_branch_instance_id":*build_branch_instance_id, "order":*new_order}
	next_build_branch_instance_step_where_map := json.NewMapOfValues(&next_build_branch_instance_step_where)

	next_build_branch_instance_step_where_logic := map[string]interface{}{"order":"="}
	next_build_branch_instance_step_where_logic_map := json.NewMapOfValues(&next_build_branch_instance_step_where_logic)

	next_build_branch_instance_step_where_request := map[string]interface{}{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId()}
	next_build_branch_instance_step_where_request_map := json.NewMapOfValues(&next_build_branch_instance_step_where_request)
	next_build_branch_instance_step_where_request_map.SetArray("[select_fields]", next_build_branch_instance_step_select_array)
	next_build_branch_instance_step_where_request_map.SetMap("[where_fields]", next_build_branch_instance_step_where_map)
	next_build_branch_instance_step_where_request_map.SetMap("[where_fields_logic]", next_build_branch_instance_step_where_logic_map)

	//next_read_records_build_branch_instance_step_request := json.Map{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order", "parameters"}, "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id, "order":*new_order}, "[where_fields_logic]":json.Map{"order":"="}}
	next_read_records_build_branch_instance_step_response, next_read_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(next_build_branch_instance_step_where_request_map)
	if next_read_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, next_read_records_build_branch_instance_step_response_errors...)
	} else if common.IsNil(next_read_records_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("next_read_records_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	next_branch_instance_steps_array, next_branch_instance_steps_array_errors := next_read_records_build_branch_instance_step_response.GetArray("data")
	if next_branch_instance_steps_array_errors != nil {
		errors = append(errors, next_branch_instance_steps_array_errors...)
	} else if common.IsNil(next_branch_instance_steps_array) {
		errors = append(errors, fmt.Errorf("next_branch_instance_steps_array is nil"))
	}

	if len(errors) > 0 {
		return errors
	}
	
	for _, next_branch_instance_step := range *(next_branch_instance_steps_array.GetValues()) {
		next_build_step, next_build_step_errors := next_branch_instance_step.GetMap()
		if next_build_step_errors != nil {
			errors = append(errors, next_build_step_errors...)
		} else if common.IsNil(next_build_step) {
			errors = append(errors, fmt.Errorf("next_build_step is nil"))
		}

		if len(errors) > 0 {
			return errors
		}

		desired_build_step_id, desired_build_step_id_errors := next_build_step.GetUInt64("build_step_id")
		if desired_build_step_id_errors != nil {
			errors = append(errors, desired_build_step_id_errors...)
		} else if common.IsNil(desired_build_step_id) {
			errors = append(errors, fmt.Errorf("build_step_id is nil"))
		}

		if len(errors) > 0 {
			return errors
		}

		next_build_steps_select := []string{"name"}
		next_build_steps_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&next_build_steps_select))

		next_build_steps_where := map[string]interface{}{"build_step_id":*desired_build_step_id}
		next_build_steps_where_map := json.NewMapOfValues(&next_build_steps_where)

		next_build_steps_where_request := map[string]interface{}{"[queue]":"ReadRecords_BuildStep", "[trace_id]":processor.GenerateTraceId()}
		next_build_steps_where_request_map := json.NewMapOfValues(&next_build_steps_where_request)
		next_build_steps_where_request_map.SetArray("[select_fields]", next_build_steps_select_array)
		next_build_steps_where_request_map.SetMap("[where_fields]", next_build_steps_where_map)

		//read_records_build_step_request := json.Map{"[queue]":"ReadRecords_BuildStep", "[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"build_step_id":*desired_build_step_id}, "[select_fields]": json.Array{"name"}}
		read_records_build_step_response, read_records_build_step_response_errors := processor.SendMessageToQueue(next_build_steps_where_request_map)
		if read_records_build_step_response_errors != nil {
			errors = append(errors, read_records_build_step_response_errors...)
		} else if common.IsNil(read_records_build_step_response) {
			errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
		}

		if len(errors) > 0 {
			return errors
		}

		lookup_build_step_array, lookup_build_step_array_errors := read_records_build_step_response.GetArray("data")
		if lookup_build_step_array_errors != nil {
			errors = append(errors, lookup_build_step_array_errors...)
		} else if common.IsNil(lookup_build_step_array) {
			errors = append(errors, fmt.Errorf("lookup_build_step_array is nil"))
		} else if len(*(lookup_build_step_array.GetValues())) != 1 {
			errors = append(errors, fmt.Errorf("lookup_build_step_array does not have one element"))
		}

		if len(errors) > 0 {
			return errors
		}

		build_step, build_step_errors := (*(lookup_build_step_array.GetValues()))[0].GetMap()
		if build_step_errors != nil {
			errors = append(errors, build_step_errors...)
		} else if common.IsNil(build_step) {
			errors = append(errors, fmt.Errorf("build_step is nil"))
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

		next_build_step.SetString("domain_name", domain_name)
		next_build_step.SetString("repository_account_name", repository_account_name)
		next_build_step.SetString("repository_name", repository_name)
		next_build_step.SetString("branch_name", branch_name)
		next_build_step.SetString("command_name", name_of_next_step)
		next_build_step.SetUInt64("build_branch_id", build_branch_id)

		next_command := map[string]interface{}{"[queue]":*name_of_next_step,"[queue_mode]":"PushBack","[async]":true, "[trace_id]":processor.GenerateTraceId()}
		next_command_map := json.NewMapOfValues(&next_command)
		next_command_map.SetMap("data", next_build_step)
	
		//next_command := json.Map{"[queue]":*name_of_next_step, "data":next_build_step,"[queue_mode]":"PushBack","[async]":true, "[trace_id]":processor.GenerateTraceId()}
		go processor.SendMessageToQueueFireAndForget(next_command_map)
	}

	return nil
}