package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func triggerNextRunCommand(processor *Processor, command_name *string, build_branch_id *uint64, build_branch_instance_step_id *uint64, build_branch_instance_id *uint64, build_step_id  *uint64, order  *int64, domain_name *string, repository_account_name *string, repository_name *string, branch_name *string, request *json.Map) ([]error) {
	var errors []error
	if command_name == nil {
		errors = append(errors, fmt.Errorf("current command_name is nil"))
		return errors
	} else if *command_name == "Run_End" {
		return nil
	}
	
	read_records_build_branch_instance_step_request := json.Map{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order"}, "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id, "order":*order}, "[where_fields_logic]":json.Map{"order":">"}, "[order_by]":json.Array{json.Map{"order":"ascending"}}, "[limit]":1}
	read_records_build_branch_instance_step_response, read_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(&read_records_build_branch_instance_step_request)
	if read_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, read_records_build_branch_instance_step_response_errors...)
	} else if common.IsNil(read_records_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}
	
	first_build_step_array, first_build_step_array_errors := read_records_build_branch_instance_step_response.GetArray("data")
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

	read_records_build_step_request := json.Map{"[queue]":"ReadRecords_BuildStep", "[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"build_step_id":*desired_build_step_id}, "[select_fields]": json.Array{"name"}, "[limit]":1}
	read_records_build_step_response, read_records_build_step_response_errors := processor.SendMessageToQueue(&read_records_build_step_request)
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

	first_build_step.SetString("domain_name", domain_name)
	first_build_step.SetString("repository_account_name", repository_account_name)
	first_build_step.SetString("repository_name", repository_name)
	first_build_step.SetString("branch_name", branch_name)
	first_build_step.SetString("command_name", name_of_next_step)
	first_build_step.SetUInt64("build_branch_id", build_branch_id)

	next_command := json.Map{"[queue]":*name_of_next_step, "data":first_build_step,"[queue_mode]":"PushBack","[async]":false, "[trace_id]":processor.GenerateTraceId()}
	_, message_errors := processor.SendMessageToQueue(&next_command)
	if message_errors != nil {
		return message_errors
	}

	return nil
}