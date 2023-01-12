package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func commandRunSync(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors := validateRunCommandHeaders(request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} else if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	previous_read_record_build_branch_instance_step_select := []string{"build_branch_instance_step_id", "build_step_status_id"}
	previous_read_record_build_branch_instance_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&previous_read_record_build_branch_instance_step_select))

	previous_read_record_build_branch_instance_step_where := map[string]interface{}{"build_branch_instance_id":*build_branch_instance_id, "order":*order}
	previous_read_record_build_branch_instance_step_where_map := json.NewMapOfValues(&previous_read_record_build_branch_instance_step_where)

	previous_read_record_build_branch_instance_step_where_logic := map[string]interface{}{"order":"<"}
	previous_read_record_build_branch_instance_step_where_logic_map := json.NewMapOfValues(&previous_read_record_build_branch_instance_step_where_logic)

	previous_read_record_build_branch_instance_step_order_by :=  map[string]interface{}{"order":"decending"}
	previous_read_record_build_branch_instance_step_order_by_map := json.NewMapOfValues(&previous_read_record_build_branch_instance_step_order_by)
	previous_read_record_build_branch_instance_step_order_by_array := json.NewArray()
	previous_read_record_build_branch_instance_step_order_by_array.AppendMap(previous_read_record_build_branch_instance_step_order_by_map)

	previous_read_record_build_branch_instance_step_request := map[string]interface{}{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	previous_read_record_build_branch_instance_step_request_map := json.NewMapOfValues(&previous_read_record_build_branch_instance_step_request)
	previous_read_record_build_branch_instance_step_request_map.SetArray("[select_fields]", previous_read_record_build_branch_instance_step_select_array)
	previous_read_record_build_branch_instance_step_request_map.SetMap("[where_fields]", previous_read_record_build_branch_instance_step_where_map)
	previous_read_record_build_branch_instance_step_request_map.SetMap("[where_fields_logic]", previous_read_record_build_branch_instance_step_where_logic_map)
	previous_read_record_build_branch_instance_step_request_map.SetArray("[order_by]", previous_read_record_build_branch_instance_step_order_by_array)


	//previous_read_record_build_branch_instance_step_request := json.Map{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_step_status_id"}, "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id, "order":*order}, "[where_fields_logic]":json.Map{"order":"<"}, "[order_by]":json.Array{json.Map{"order":"decending"}}, "[limit]":1}
	previous_read_record_build_branch_instance_step_response, previous_read_record_build_branch_instance_step_response_errors := processor.SendMessageToQueue(previous_read_record_build_branch_instance_step_request_map)
	if previous_read_record_build_branch_instance_step_response_errors != nil {
		errors = append(errors, previous_read_record_build_branch_instance_step_response_errors...)
	} else if common.IsNil(previous_read_record_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("previous_read_record_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	previous_step_array, previous_step_array_errors := previous_read_record_build_branch_instance_step_response.GetArray("data")
	if previous_step_array_errors != nil {
		errors = append(errors, previous_step_array_errors...)
	} else if common.IsNil(previous_step_array) {
		errors = append(errors, fmt.Errorf("previous_step_array is nil"))
	} else if len(*(previous_step_array.GetValues())) == 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		} 

		if len(errors) > 0 {
			return errors
		} else {
			return nil
		}
	} 

	prevous_step, prevous_step_errors := (*(previous_step_array.GetValues()))[0].GetMap()
	if prevous_step_errors != nil {
		errors = append(errors, prevous_step_errors...)
	} else if common.IsNil(prevous_step) {
		errors = append(errors, fmt.Errorf("prevous_step is nil"))
	}

	if len(errors) > 0 {
		return errors
	}


	previous_order, previous_order_errors := prevous_step.GetInt64("order")
	if previous_order_errors != nil {
		errors = append(errors, previous_order_errors...)
	} else if common.IsNil(previous_order) {
		errors = append(errors, fmt.Errorf("previous_order is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	previous_instance_steps_select := []string{"build_branch_instance_step_id", "build_step_status_id"}
	previous_instance_steps_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&previous_instance_steps_select))

	previous_instance_steps_where := map[string]interface{}{"build_branch_instance_id":*build_branch_instance_id, "order":*previous_order}
	previous_instance_steps_where_map := json.NewMapOfValues(&previous_instance_steps_where)

	previous_instance_steps_where_logic := map[string]interface{}{"order":"="}
	previous_instance_steps_where_logic_map := json.NewMapOfValues(&previous_instance_steps_where_logic)

	previous_instance_steps_order_by :=  map[string]interface{}{"order":"decending"}
	previous_instance_steps_order_by_map := json.NewMapOfValues(&previous_instance_steps_order_by)
	previous_instance_steps_order_by_array := json.NewArray()
	previous_instance_steps_order_by_array.AppendMap(previous_instance_steps_order_by_map)

	previous_instance_steps_request := map[string]interface{}{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId()}
	previous_instance_steps_request_map := json.NewMapOfValues(&previous_instance_steps_request)
	previous_instance_steps_request_map.SetArray("[select_fields]", previous_instance_steps_select_array)
	previous_instance_steps_request_map.SetMap("[where_fields]", previous_instance_steps_where_map)
	previous_instance_steps_request_map.SetMap("[where_fields_logic]", previous_instance_steps_where_logic_map)
	previous_instance_steps_request_map.SetArray("[order_by]", previous_instance_steps_order_by_array)

	//previous_read_records_build_branch_instance_step_request := json.Map{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_step_status_id"}, "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id, "order":*previous_order}, "[where_fields_logic]":json.Map{"order":"="}}
	previous_read_records_build_branch_instance_step_response, previous_read_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(previous_instance_steps_request_map)
	if previous_read_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, previous_read_records_build_branch_instance_step_response_errors...)
	} else if common.IsNil(previous_read_records_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
	} 	

	if len(errors) > 0 {
		return errors
	}

	previous_read_records_build_branch_instance_step_array, previous_read_records_build_branch_instance_step_array_errors := previous_read_records_build_branch_instance_step_response.GetArray("data")
	if previous_read_records_build_branch_instance_step_array_errors != nil {
		errors = append(errors, previous_read_records_build_branch_instance_step_array_errors...)
	} else if common.IsNil(previous_read_records_build_branch_instance_step_array) {
		errors = append(errors, fmt.Errorf("previous_read_records_build_branch_instance_step_array is nil"))
	} else if len(*(previous_read_records_build_branch_instance_step_array.GetValues())) == 0 {
		errors = append(errors, fmt.Errorf("previous_read_records_build_branch_instance_step_array return 0 records"))
	}

	if len(errors) > 0 {
		return errors
	}

	not_started_select := []string{"build_step_status_id"}
	not_started_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&not_started_select))

	not_started_where := map[string]interface{}{"name":"Not Started"}
	not_started_where_map := json.NewMapOfValues(&not_started_where)

	not_started_request := map[string]interface{}{"[queue]":"ReadRecords_BuildStepStatus", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	not_started_request_map := json.NewMapOfValues(&not_started_request)
	not_started_request_map.SetArray("[select_fields]", not_started_select_array)
	not_started_request_map.SetMap("[where_fields]", not_started_where_map)

	//read_records_build_step_status_not_started_request := json.Map{"[queue]":"ReadRecords_BuildStepStatus", "[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"name":"Not Started"}, "[select_fields]": json.Array{"build_step_status_id"}, "[limit]":1}
	read_records_build_step_status_not_started_response, read_records_build_step_status_not_started_response_errors := processor.SendMessageToQueue(not_started_request_map)
	if read_records_build_step_status_not_started_response_errors != nil {
		errors = append(errors, read_records_build_step_status_not_started_response_errors...)
	} else if common.IsNil(read_records_build_step_status_not_started_response) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_not_started_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_build_step_status_not_stated_array, read_records_build_step_status_not_stated_array_errors := read_records_build_step_status_not_started_response.GetArray("data")
	if read_records_build_step_status_not_stated_array_errors != nil {
		errors = append(errors, read_records_build_step_status_not_stated_array_errors...)
	} else if common.IsNil(read_records_build_step_status_not_stated_array) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_not_stated_array is nil"))
	} else if len(*(read_records_build_step_status_not_stated_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_not_stated_array did not return 1 record"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_step_status_not_started, build_step_status_not_started_errors :=  (*(read_records_build_step_status_not_stated_array.GetValues()))[0].GetMap()
	if build_step_status_not_started_errors != nil {
		errors = append(errors, build_step_status_not_started_errors...)
	} else if common.IsNil(build_step_status_not_started) {
		errors = append(errors, fmt.Errorf("build_step_status_not_started is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_step_status_not_started_id, build_step_status_not_started_id_errors := build_step_status_not_started.GetUInt64("build_step_status_id")
	if build_step_status_not_started_id_errors != nil {
		errors = append(errors, build_step_status_not_started_id_errors...)
	} else if common.IsNil(build_step_status_not_started_id) {
		errors = append(errors, fmt.Errorf("build_step_status_not_started_id is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	running_select := []string{"build_step_status_id"}
	running_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&running_select))

	running_where := map[string]interface{}{"name":"Running"}
	running_where_map := json.NewMapOfValues(&running_where)

	running_request := map[string]interface{}{"[queue]":"ReadRecords_BuildStepStatus", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	running_request_map := json.NewMapOfValues(&running_request)
	running_request_map.SetArray("[select_fields]", running_select_array)
	running_request_map.SetMap("[where_fields]", running_where_map)


	//read_records_build_step_status_running_request := json.Map{"[queue]":"ReadRecords_BuildStepStatus", "[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"name":"Running"}, "[select_fields]": json.Array{"build_step_status_id"}, "[limit]":1}
	read_records_build_step_status_running_response, read_records_build_step_status_running_response_errors := processor.SendMessageToQueue(running_request_map)
	if read_records_build_step_status_running_response_errors != nil {
		errors = append(errors, read_records_build_step_status_running_response_errors...)
	} else if common.IsNil(read_records_build_step_status_running_response) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_running_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_build_step_status_running_array, read_records_build_step_status_running_array_errors := read_records_build_step_status_running_response.GetArray("data")
	if read_records_build_step_status_running_array_errors != nil {
		errors = append(errors, read_records_build_step_status_running_array_errors...)
	} else if common.IsNil(read_records_build_step_status_running_array) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_running_array is nil"))
	} else if len(*(read_records_build_step_status_running_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_running_array did not return 1 record"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_step_status_running, build_step_status_running_errors := (*(read_records_build_step_status_running_array.GetValues()))[0].GetMap()
	if build_step_status_running_errors != nil {
		errors = append(errors, build_step_status_running_errors...)
	} else if common.IsNil(build_step_status_running) {
		errors = append(errors, fmt.Errorf("build_step_status_running is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_step_status_running_id, build_step_status_running_id_errors := build_step_status_running.GetUInt64("build_step_status_id")
	if build_step_status_running_id_errors != nil {
		errors = append(errors, build_step_status_running_id_errors...)
	} else if common.IsNil(build_step_status_running_id) {
		errors = append(errors, fmt.Errorf("build_step_status_running_id is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	incomplete_steps_found := false
	incomplete_steps_found_running_count := 0
	incomplete_steps_found_not_started_count := 0
	for _, previous_read_records_build_branch_instance_step := range *(previous_read_records_build_branch_instance_step_array.GetValues()) {
		compare_step, compare_step_errors  := previous_read_records_build_branch_instance_step.GetMap()
		if compare_step_errors != nil {
			errors = append(errors, compare_step_errors...)
		} else if common.IsNil(compare_step) {
			errors = append(errors, fmt.Errorf("compare_step is nil"))
		}
		if len(errors) > 0 {
			return errors
		}

		compare_step_build_step_status_id, compare_step_build_step_status_id_errors := compare_step.GetUInt64("build_step_status_id")
		if compare_step_build_step_status_id_errors != nil {
			errors = append(errors, compare_step_build_step_status_id_errors...)
		} else if common.IsNil(compare_step_build_step_status_id) {
			errors = append(errors, fmt.Errorf("compare_step_build_step_status_id is nil"))
		} 

		if len(errors) > 0 {
			return errors
		}

		if *compare_step_build_step_status_id == *build_step_status_running_id {
			incomplete_steps_found = true
			incomplete_steps_found_running_count++
		}

		if *compare_step_build_step_status_id == *build_step_status_not_started_id {
			incomplete_steps_found = true
			incomplete_steps_found_not_started_count++
		}
	}

	if incomplete_steps_found {
		fmt.Println(fmt.Sprintf("there are %d steps with status \"Running\" and %d steps with status \"Not Started\" ignore triggering next step", incomplete_steps_found_running_count, incomplete_steps_found_not_started_count))
		return nil
	}

	if len(errors) == 0 && !incomplete_steps_found {
		fmt.Println("all previous steps have completed... triggering next step")
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
	}
	
	if len(errors) > 0 {
		return errors
	}

	return nil
}

func commandRunSyncFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunSync
	return &funcValue
}