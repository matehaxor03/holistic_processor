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
		fmt.Println("1")
		fmt.Println(errors)
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		fmt.Println(errors)
		return errors
	}


	previous_read_record_build_branch_instance_step_request := json.Map{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_step_status_id"}, "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id, "order":*order}, "[where_fields_logic]":json.Map{"order":"<"}, "[order_by]":json.Array{json.Map{"order":"decending"}}, "[limit]":1}
	previous_read_record_build_branch_instance_step_response, previous_read_record_build_branch_instance_step_response_errors := processor.SendMessageToQueue(&previous_read_record_build_branch_instance_step_request)
	if previous_read_record_build_branch_instance_step_response_errors != nil {
		errors = append(errors, previous_read_record_build_branch_instance_step_response_errors...)
	} else if common.IsNil(previous_read_record_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("previous_read_record_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		fmt.Println("2")
		fmt.Println(errors)
		return errors
	}

	previous_step_array, previous_step_array_errors := previous_read_record_build_branch_instance_step_response.GetArray("data")
	if previous_step_array_errors != nil {
		errors = append(errors, previous_step_array_errors...)
	} else if common.IsNil(previous_step_array) {
		errors = append(errors, fmt.Errorf("previous_step_array is nil"))
	} else if len(*previous_step_array) == 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		} 

		if len(errors) > 0 {
			fmt.Println("3")
			fmt.Println(errors)
			return errors
		} else {
			return nil
		}
	} 

	var prevous_step json.Map
	prevous_step_interface := (*previous_step_array)[0]
	type_of_previous_step := common.GetType(prevous_step_interface)

	if type_of_previous_step == "json.Map" {
		prevous_step = prevous_step_interface.(json.Map)
	} else if type_of_previous_step == "*json.Map" {
		prevous_step = *(prevous_step_interface.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("previous step has invalid type"))
	}

	if len(errors) > 0 {
		fmt.Println("4")
		fmt.Println(errors)
		return errors
	}


	previous_order, previous_order_errors := prevous_step.GetInt64("order")
	if previous_order_errors != nil {
		errors = append(errors, previous_order_errors...)
	} else if common.IsNil(previous_order) {
		errors = append(errors, fmt.Errorf("previous_order is nil"))
	}

	if len(errors) > 0 {
		fmt.Println("5")
		fmt.Println(errors)
		return errors
	}

	fmt.Println(*previous_order)

	previous_read_records_build_branch_instance_step_request := json.Map{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_step_status_id"}, "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id, "order":*previous_order}, "[where_fields_logic]":json.Map{"order":"="}}
	previous_read_records_build_branch_instance_step_response, previous_read_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(&previous_read_records_build_branch_instance_step_request)
	if previous_read_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, previous_read_records_build_branch_instance_step_response_errors...)
	} else if common.IsNil(previous_read_records_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
	} 	

	if len(errors) > 0 {
		fmt.Println("6")
		fmt.Println(errors)
		return errors
	}

	previous_read_records_build_branch_instance_step_array, previous_read_records_build_branch_instance_step_array_errors := previous_read_records_build_branch_instance_step_response.GetArray("data")
	if previous_read_records_build_branch_instance_step_array_errors != nil {
		errors = append(errors, previous_read_records_build_branch_instance_step_array_errors...)
	} else if common.IsNil(previous_read_records_build_branch_instance_step_array) {
		errors = append(errors, fmt.Errorf("previous_read_records_build_branch_instance_step_array is nil"))
	} else if len(*previous_read_records_build_branch_instance_step_array) == 0 {
		errors = append(errors, fmt.Errorf("previous_read_records_build_branch_instance_step_array return 0 records"))
	}

	if len(errors) > 0 {
		fmt.Println("7")
		fmt.Println(errors)
		return errors
	}

	read_records_build_step_status_not_started_request := json.Map{"[queue]":"ReadRecords_BuildStepStatus", "[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"name":"Not Started"}, "[select_fields]": json.Array{"build_step_status_id"}, "[limit]":1}
	read_records_build_step_status_not_started_response, read_records_build_step_status_not_started_response_errors := processor.SendMessageToQueue(&read_records_build_step_status_not_started_request)
	if read_records_build_step_status_not_started_response_errors != nil {
		errors = append(errors, read_records_build_step_status_not_started_response_errors...)
	} else if common.IsNil(read_records_build_step_status_not_started_response) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_not_started_response is nil"))
	}

	if len(errors) > 0 {
		fmt.Println("8")
		fmt.Println(errors)
		return errors
	}

	read_records_build_step_status_not_stated_array, read_records_build_step_status_not_stated_array_errors := read_records_build_step_status_not_started_response.GetArray("data")
	if read_records_build_step_status_not_stated_array_errors != nil {
		errors = append(errors, read_records_build_step_status_not_stated_array_errors...)
	} else if common.IsNil(read_records_build_step_status_not_stated_array) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_not_stated_array is nil"))
	} else if len(*read_records_build_step_status_not_stated_array) != 1 {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_not_stated_array did not return 1 record"))
	}

	if len(errors) > 0 {
		fmt.Println("9")
		fmt.Println(errors)
		return errors
	}

	build_step_status_not_started := json.Map{}
	build_step_status_not_started_instance := (*read_records_build_step_status_not_stated_array)[0]
	type_of_build_step_status_not_started := common.GetType(build_step_status_not_started_instance)

	if type_of_build_step_status_not_started == "json.Map" {
		build_step_status_not_started = build_step_status_not_started_instance.(json.Map)
	} else if type_of_build_step_status_not_started == "*json.Map" {
		build_step_status_not_started = *(build_step_status_not_started_instance.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("build_step_status_not_started has invalid type"))
	}

	if len(errors) > 0 {
		fmt.Println("10")
		fmt.Println(errors)
		return errors
	}

	build_step_status_not_started_id, build_step_status_not_started_id_errors := build_step_status_not_started.GetUInt64("build_step_status_id")
	if build_step_status_not_started_id_errors != nil {
		errors = append(errors, build_step_status_not_started_id_errors...)
	} else if common.IsNil(build_step_status_not_started_id) {
		errors = append(errors, fmt.Errorf("build_step_status_not_started_id is nil"))
	}

	if len(errors) > 0 {
		fmt.Println("11")
		fmt.Println(errors)
		return errors
	}

	read_records_build_step_status_running_request := json.Map{"[queue]":"ReadRecords_BuildStepStatus", "[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"name":"Running"}, "[select_fields]": json.Array{"build_step_status_id"}, "[limit]":1}
	read_records_build_step_status_running_response, read_records_build_step_status_running_response_errors := processor.SendMessageToQueue(&read_records_build_step_status_running_request)
	if read_records_build_step_status_running_response_errors != nil {
		errors = append(errors, read_records_build_step_status_running_response_errors...)
	} else if common.IsNil(read_records_build_step_status_running_response) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_running_response is nil"))
	}

	if len(errors) > 0 {
		fmt.Println("12")
		fmt.Println(errors)
		return errors
	}

	read_records_build_step_status_running_array, read_records_build_step_status_running_array_errors := read_records_build_step_status_running_response.GetArray("data")
	if read_records_build_step_status_running_array_errors != nil {
		errors = append(errors, read_records_build_step_status_running_array_errors...)
	} else if common.IsNil(read_records_build_step_status_running_array) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_running_array is nil"))
	} else if len(*read_records_build_step_status_running_array) != 1 {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_running_array did not return 1 record"))
	}

	if len(errors) > 0 {
		fmt.Println("13")
		fmt.Println(errors)
		return errors
	}

	build_step_status_running := json.Map{}
	build_step_status_running_instance := (*read_records_build_step_status_running_array)[0]
	type_of_build_step_running_started := common.GetType(build_step_status_running_instance)

	if type_of_build_step_running_started == "json.Map" {
		build_step_status_running = build_step_status_running_instance.(json.Map)
	} else if type_of_build_step_running_started == "*json.Map" {
		build_step_status_running = *(build_step_status_running_instance.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("build_step_status_running has invalid type"))
	}

	if len(errors) > 0 {
		fmt.Println("14")
		fmt.Println(errors)
		return errors
	}

	build_step_status_running_id, build_step_status_running_id_errors := build_step_status_running.GetUInt64("build_step_status_id")
	if build_step_status_running_id_errors != nil {
		errors = append(errors, build_step_status_running_id_errors...)
	} else if common.IsNil(build_step_status_running_id) {
		errors = append(errors, fmt.Errorf("build_step_status_running_id is nil"))
	}

	if len(errors) > 0 {
		fmt.Println("15")
		fmt.Println(errors)
		return errors
	}


	for _, previous_read_records_build_branch_instance_step := range *previous_read_records_build_branch_instance_step_array {
		compare_step := json.Map{}
		compare_step_instance := previous_read_records_build_branch_instance_step
		type_of_compare_step := common.GetType(compare_step_instance)

		if type_of_compare_step == "json.Map" {
			compare_step = compare_step_instance.(json.Map)
		} else if type_of_compare_step == "*json.Map" {
			compare_step = *(compare_step_instance.(*json.Map))
		} else {
			errors = append(errors, fmt.Errorf("compare_step has invalid type"))
		}

		if len(errors) > 0 {
			fmt.Println("16")
			fmt.Println(errors)
			return errors
		}

		compare_step_build_step_status_id, compare_step_build_step_status_id_errors := compare_step.GetUInt64("build_step_status_id")
		if compare_step_build_step_status_id_errors != nil {
			errors = append(errors, compare_step_build_step_status_id_errors...)
		} else if common.IsNil(compare_step_build_step_status_id) {
			errors = append(errors, fmt.Errorf("compare_step_build_step_status_id is nil"))
		} 

		if len(errors) > 0 {
			fmt.Println("17")
			fmt.Println(errors)
			return errors
		}

		fmt.Println(*compare_step_build_step_status_id)


		if *compare_step_build_step_status_id == *build_step_status_running_id {
			fmt.Println("there are steps still running")
			return nil
		}

		if *compare_step_build_step_status_id == *build_step_status_not_started_id {
			fmt.Println("there are steps not started running")
			return nil
		}
	}

	if len(errors) == 0 {
		fmt.Println("18")
		fmt.Println(errors)
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
	}
	
	if len(errors) > 0 {
		fmt.Println("19")
		fmt.Println(errors)
		return errors
	}

	return nil
}

func commandRunSyncFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunSync
	return &funcValue
}