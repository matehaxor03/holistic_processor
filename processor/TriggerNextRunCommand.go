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

	read_records_build_step_status_request := json.Map{"[queue]":"ReadRecords_BuildStepStatus", "[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"name":lookup_name}, "[select_fields]": json.Array{"build_step_status_id"}, "[limit]":1}
	read_records_build_step_status_response, read_records_build_step_status_response_errors := processor.SendMessageToQueue(&read_records_build_step_status_request)
	if read_records_build_step_status_response_errors != nil {
		errors = append(errors, read_records_build_step_status_response_errors...)
	} else if common.IsNil(read_records_build_step_status_response) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_response is nil"))
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}

	read_records_build_step_status_array, read_records_build_step_status_array_errors := read_records_build_step_status_response.GetArray("data")
	if read_records_build_step_status_array_errors != nil {
		errors = append(errors, read_records_build_step_status_array_errors...)
	} else if common.IsNil(read_records_build_step_status_array) {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_array is nil"))
	} else if len(*read_records_build_step_status_array) != 1 {
		errors = append(errors, fmt.Errorf("read_records_build_step_status_array did not return 1 record"))
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}

	build_step_status := json.Map{}
	build_step_status_instance := (*read_records_build_step_status_array)[0]
	type_of_build_step_status := common.GetType(build_step_status_instance)

	if type_of_build_step_status == "json.Map" {
		build_step_status = build_step_status_instance.(json.Map)
	} else if type_of_build_step_status == "*json.Map" {
		build_step_status = *(build_step_status_instance.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("build_step_status has invalid type"))
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}

	build_step_status_id, build_step_status_id_errors := build_step_status.GetUInt64("build_step_status_id")
	if build_step_status_id_errors != nil {
		errors = append(errors, build_step_status_id_errors...)
	} else if common.IsNil(build_step_status_id) {
		errors = append(errors, fmt.Errorf("build_step_status_id is nil"))
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}
	
	update_records_build_branch_instance_step_request := json.Map{"[queue]":"UpdateRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "data": json.Array{json.Map{"build_branch_instance_step_id":build_branch_instance_step_id, "build_step_status_id":*build_step_status_id}}}
	_, update_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(&update_records_build_branch_instance_step_request)
	if update_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, update_records_build_branch_instance_step_response_errors...)
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}
	
	read_records_build_branch_instance_step_request := json.Map{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order"}, "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id, "order":*order}, "[where_fields_logic]":json.Map{"order":">"}, "[order_by]":json.Array{json.Map{"order":"ascending"}}, "[limit]":1}
	read_records_build_branch_instance_step_response, read_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(&read_records_build_branch_instance_step_request)
	if read_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, read_records_build_branch_instance_step_response_errors...)
	} else if common.IsNil(read_records_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}
	
	next_branch_instance_steps, next_branch_instance_steps_errors := read_records_build_branch_instance_step_response.GetArray("data")
	if next_branch_instance_steps_errors != nil {
		errors = append(errors, next_branch_instance_steps_errors...)
	} else if common.IsNil(next_branch_instance_steps) {
		errors = append(errors, fmt.Errorf("next_branch_instance_steps is nil"))
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}

	if len(*next_branch_instance_steps) == 0 {
		return nil
	}

	var next_branch_instance_build_step json.Map
	next_branch_instance_step := (*next_branch_instance_steps)[0]
	type_of_next_branch_instance_build_step := common.GetType(next_branch_instance_step)

	if type_of_next_branch_instance_build_step == "json.Map" {
		next_branch_instance_build_step = next_branch_instance_step.(json.Map)
	} else if type_of_next_branch_instance_build_step == "*json.Map" {
		next_branch_instance_build_step = *(next_branch_instance_step.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("first build step has invalid type"))
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}

	new_order, new_order_errors := next_branch_instance_build_step.GetInt64("order")
	if new_order_errors != nil {
		errors = append(errors, new_order_errors...)
	} else if common.IsNil(new_order) {
		errors = append(errors, fmt.Errorf("new_order is nil"))
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}

	next_read_records_build_branch_instance_step_request := json.Map{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order", "parameters"}, "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id, "order":*new_order}, "[where_fields_logic]":json.Map{"order":"="}}
	next_read_records_build_branch_instance_step_response, next_read_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(&next_read_records_build_branch_instance_step_request)
	if next_read_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, next_read_records_build_branch_instance_step_response_errors...)
	} else if common.IsNil(next_read_records_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("next_read_records_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}

	next_branch_instance_steps_array, next_branch_instance_steps_array_errors := next_read_records_build_branch_instance_step_response.GetArray("data")
	if next_branch_instance_steps_array_errors != nil {
		errors = append(errors, next_branch_instance_steps_array_errors...)
	} else if common.IsNil(next_branch_instance_steps_array) {
		errors = append(errors, fmt.Errorf("next_branch_instance_steps_array is nil"))
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	}
	
	for _, next_branch_instance_step := range *next_branch_instance_steps_array {
		next_build_step := json.Map{}
		type_of_next_build_step := common.GetType(next_branch_instance_step)

		if type_of_next_build_step == "json.Map" {
			next_build_step = next_branch_instance_step.(json.Map)
		} else if type_of_next_build_step == "*json.Map" {
			next_build_step = *(next_branch_instance_step.(*json.Map))
		} else {
			errors = append(errors, fmt.Errorf("first build step has invalid type"))
		}

		if len(errors) > 0 {
			fmt.Println(errors)
			return errors
		}

		desired_build_step_id, desired_build_step_id_errors := next_build_step.GetUInt64("build_step_id")
		if desired_build_step_id_errors != nil {
			errors = append(errors, desired_build_step_id_errors...)
		} else if common.IsNil(desired_build_step_id) {
			errors = append(errors, fmt.Errorf("build_step_id is nil"))
		}

		if len(errors) > 0 {
			fmt.Println(errors)
			return errors
		}

		read_records_build_step_request := json.Map{"[queue]":"ReadRecords_BuildStep", "[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"build_step_id":*desired_build_step_id}, "[select_fields]": json.Array{"name"}}
		read_records_build_step_response, read_records_build_step_response_errors := processor.SendMessageToQueue(&read_records_build_step_request)
		if read_records_build_step_response_errors != nil {
			errors = append(errors, read_records_build_step_response_errors...)
		} else if common.IsNil(read_records_build_step_response) {
			errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
		}

		if len(errors) > 0 {
			fmt.Println(errors)
			return errors
		}

		lookup_build_step_array, lookup_build_step_array_errors := read_records_build_step_response.GetArray("data")
		if lookup_build_step_array_errors != nil {
			errors = append(errors, lookup_build_step_array_errors...)
		} else if common.IsNil(lookup_build_step_array) {
			errors = append(errors, fmt.Errorf("lookup_build_step_array is nil"))
		} else if len(*lookup_build_step_array) != 1 {
			errors = append(errors, fmt.Errorf("lookup_build_step_array does not have one element"))
		}

		if len(errors) > 0 {
			fmt.Println(errors)
			return errors
		}

		build_step := json.Map{}
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
			fmt.Println(errors)
			return errors
		}

		name_of_next_step, name_of_next_step_errors := build_step.GetString("name")
		if name_of_next_step_errors != nil {
			errors = append(errors, name_of_next_step_errors...)
		} else if common.IsNil(name_of_next_step) {
			errors = append(errors, fmt.Errorf("name attribute for next step is nil"))
		}

		if len(errors) > 0 {
			fmt.Println(errors)
			return errors
		}

		next_build_step.SetString("domain_name", domain_name)
		next_build_step.SetString("repository_account_name", repository_account_name)
		next_build_step.SetString("repository_name", repository_name)
		next_build_step.SetString("branch_name", branch_name)
		next_build_step.SetString("command_name", name_of_next_step)
		next_build_step.SetUInt64("build_branch_id", build_branch_id)

		next_command := json.Map{"[queue]":*name_of_next_step, "data":next_build_step,"[queue_mode]":"PushBack","[async]":true, "[trace_id]":processor.GenerateTraceId()}
		go processor.SendMessageToQueueFireAndForget(&next_command)
	}

	return nil
}