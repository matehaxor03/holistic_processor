package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func commandRunSync(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors := validateRunCommandHeaders(processor, request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} else if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}
	client_write := processor.GetClientWrite()
	database := client_write.GetDatabase()
	table_BranchInstanceStep, table_BranchInstanceStep_errors := database.GetTable("BranchInstanceStep")
	if table_BranchInstanceStep_errors != nil {
		errors = append(errors, table_BranchInstanceStep_errors...)
	} else if common.IsNil(table_BranchInstanceStep) {
		errors = append(errors, fmt.Errorf("table_BranchInstanceStep table is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	where_query_current_build_branch_instance_step := json.NewMap()
	where_query_current_build_branch_instance_step.SetUInt64("branch_instance_step_id", branch_instance_step_id)
	curent_build_branch_instanace_step_records, curent_build_branch_instanace_step_records_errors := table_BranchInstanceStep.ReadRecords(nil, where_query_current_build_branch_instance_step, nil, nil, nil, nil)
	if curent_build_branch_instanace_step_records_errors != nil {
		return curent_build_branch_instanace_step_records_errors
	} else if len(*curent_build_branch_instanace_step_records) == 0 {
		errors = append(errors, fmt.Errorf("did not find record for curent_build_branch_instanace_step_records"))
		return errors
	}  else if len(*curent_build_branch_instanace_step_records) > 1 {
		errors = append(errors, fmt.Errorf("found too many records for curent_build_branch_instanace_step_records"))
		return errors
	}
	
	build_branch_instance_step_id_record := (*curent_build_branch_instanace_step_records)[0]
	current_build_step_status_id, current_build_step_status_id_errors := build_branch_instance_step_id_record.GetUInt64("build_step_status_id")
	if current_build_step_status_id_errors != nil {
		return current_build_step_status_id_errors
	}

	table_BuildStepStatus, table_BuildStepStatus_errors := database.GetTable("BuildStepStatus")
	if table_BuildStepStatus_errors != nil {
		errors = append(errors, table_BuildStepStatus_errors...)
	} else if common.IsNil(table_BuildStepStatus) {
		errors = append(errors, fmt.Errorf("buildstep status table is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	where_query_build_step_status_not_started := json.NewMap()
	where_query_build_step_status_not_started.SetStringValue("name", "Not Started")
	records_not_started_step_status, records_not_started_step_status_errors := table_BuildStepStatus.ReadRecords(nil, where_query_build_step_status_not_started, nil, nil, nil, nil)
	if records_not_started_step_status_errors != nil {
		return records_not_started_step_status_errors
	} else if len(*records_not_started_step_status) == 0 {
		errors = append(errors, fmt.Errorf("did not find record for Not Started BuildStepStatus"))
		return errors
	}  else if len(*records_not_started_step_status) > 1 {
		errors = append(errors, fmt.Errorf("found too many records for Not Started BuildStepStatus"))
		return errors
	}

	not_started_build_step_status_id, not_started_build_step_status_id_errors := ((*records_not_started_step_status)[0]).GetUInt64("build_step_status_id")
	if not_started_build_step_status_id_errors != nil {
		return not_started_build_step_status_id_errors
	}

	where_query_build_step_status_running := json.NewMap()
	where_query_build_step_status_running.SetStringValue("name", "Running")
	records_running_step_status, records_running_step_status_errors := table_BuildStepStatus.ReadRecords(nil, where_query_build_step_status_running, nil, nil, nil, nil)
	if records_running_step_status_errors != nil {
		return records_running_step_status_errors
	} else if len(*records_running_step_status) == 0 {
		errors = append(errors, fmt.Errorf("did not find record for Running BuildStepStatus"))
		return errors
	}  else if len(*records_running_step_status) > 1 {
		errors = append(errors, fmt.Errorf("found too many records for Running BuildStepStatus"))
		return errors
	}

	running_build_step_status_id, running_build_step_status_id_errors := ((*records_running_step_status)[0]).GetUInt64("build_step_status_id")
	if running_build_step_status_id_errors != nil {
		return running_build_step_status_id_errors
	}

	if !(*current_build_step_status_id == *not_started_build_step_status_id || 
		 *current_build_step_status_id == *running_build_step_status_id) {
		fmt.Println("sync already completed skipping")
		return nil
	}

	where_query_build_step_status_passed := json.NewMap()
	where_query_build_step_status_passed.SetStringValue("name", "Passed")
	records_passed_step_status, records_passed_step_status_errors := table_BuildStepStatus.ReadRecords(nil, where_query_build_step_status_passed, nil, nil, nil, nil)
	if records_passed_step_status_errors != nil {
		return records_passed_step_status_errors
	} else if len(*records_passed_step_status) == 0 {
		errors = append(errors, fmt.Errorf("did not find record for Passed BuildStepStatus"))
		return errors
	}  else if len(*records_passed_step_status) > 1 {
		errors = append(errors, fmt.Errorf("found too many records for Passed BuildStepStatus"))
		return errors
	}

	passed_build_step_status_id, passed_build_step_status_id_errors := ((*records_passed_step_status)[0]).GetUInt64("build_step_status_id")
	if passed_build_step_status_id_errors != nil {
		return passed_build_step_status_id_errors
	}

	where_query_build_step_status_failed := json.NewMap()
	where_query_build_step_status_failed.SetStringValue("name", "Failed")
	records_failed_step_status, records_failed_step_status_errors := table_BuildStepStatus.ReadRecords(nil, where_query_build_step_status_failed, nil, nil, nil, nil)
	if records_failed_step_status_errors != nil {
		return records_failed_step_status_errors
	} else if len(*records_failed_step_status) == 0 {
		errors = append(errors, fmt.Errorf("did not find record for Failed BuildStepStatus"))
		return errors
	}  else if len(*records_failed_step_status) > 1 {
		errors = append(errors, fmt.Errorf("found too many records for Failed BuildStepStatus"))
		return errors
	}

	failed_build_step_status_id, failed_build_step_status_id_errors := ((*records_failed_step_status)[0]).GetUInt64("build_step_status_id")
	if failed_build_step_status_id_errors != nil {
		return failed_build_step_status_id_errors
	}

	previous_read_record_build_branch_instance_step_select := []string{"branch_instance_step_id", "build_step_status_id", "order"}
	previous_read_record_build_branch_instance_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&previous_read_record_build_branch_instance_step_select))

	previous_read_record_build_branch_instance_step_where := map[string]interface{}{"branch_instance_id":*branch_instance_id, "order":*order}
	previous_read_record_build_branch_instance_step_where_map := json.NewMapOfValues(&previous_read_record_build_branch_instance_step_where)

	previous_read_record_build_branch_instance_step_where_logic := map[string]interface{}{"order":"<"}
	previous_read_record_build_branch_instance_step_where_logic_map := json.NewMapOfValues(&previous_read_record_build_branch_instance_step_where_logic)

	previous_read_record_build_branch_instance_step_order_by :=  map[string]interface{}{"order":"decending"}
	previous_read_record_build_branch_instance_step_order_by_map := json.NewMapOfValues(&previous_read_record_build_branch_instance_step_order_by)
	previous_read_record_build_branch_instance_step_order_by_array := json.NewArray()
	previous_read_record_build_branch_instance_step_order_by_array.AppendMap(previous_read_record_build_branch_instance_step_order_by_map)

	previous_read_record_build_branch_instance_step_request := map[string]interface{}{"[queue]":"ReadRecords_BranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	previous_read_record_build_branch_instance_step_request_map := json.NewMapOfValues(&previous_read_record_build_branch_instance_step_request)
	previous_read_record_build_branch_instance_step_request_map.SetArray("[select_fields]", previous_read_record_build_branch_instance_step_select_array)
	previous_read_record_build_branch_instance_step_request_map.SetMap("[where_fields]", previous_read_record_build_branch_instance_step_where_map)
	previous_read_record_build_branch_instance_step_request_map.SetMap("[where_fields_logic]", previous_read_record_build_branch_instance_step_where_logic_map)
	previous_read_record_build_branch_instance_step_request_map.SetArray("[order_by]", previous_read_record_build_branch_instance_step_order_by_array)

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
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
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

	previous_instance_steps_select := []string{"branch_instance_step_id", "build_step_status_id"}
	previous_instance_steps_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&previous_instance_steps_select))

	previous_instance_steps_where := map[string]interface{}{"branch_instance_id":*branch_instance_id, "order":*previous_order}
	previous_instance_steps_where_map := json.NewMapOfValues(&previous_instance_steps_where)

	previous_instance_steps_where_logic := map[string]interface{}{"order":"="}
	previous_instance_steps_where_logic_map := json.NewMapOfValues(&previous_instance_steps_where_logic)

	previous_instance_steps_order_by :=  map[string]interface{}{"order":"decending"}
	previous_instance_steps_order_by_map := json.NewMapOfValues(&previous_instance_steps_order_by)
	previous_instance_steps_order_by_array := json.NewArray()
	previous_instance_steps_order_by_array.AppendMap(previous_instance_steps_order_by_map)

	previous_instance_steps_request := map[string]interface{}{"[queue]":"ReadRecords_BranchInstanceStep", "[trace_id]":processor.GenerateTraceId()}
	previous_instance_steps_request_map := json.NewMapOfValues(&previous_instance_steps_request)
	previous_instance_steps_request_map.SetArray("[select_fields]", previous_instance_steps_select_array)
	previous_instance_steps_request_map.SetMap("[where_fields]", previous_instance_steps_where_map)
	previous_instance_steps_request_map.SetMap("[where_fields_logic]", previous_instance_steps_where_logic_map)
	previous_instance_steps_request_map.SetArray("[order_by]", previous_instance_steps_order_by_array)

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


	incomplete_steps_found := false
	incomplete_steps_found_running_count := 0
	incomplete_steps_found_not_started_count := 0
	incomplete_steps_found_passed_count := 0
	incomplete_steps_found_failed_count := 0
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

		if *compare_step_build_step_status_id == *running_build_step_status_id {
			incomplete_steps_found = true
			incomplete_steps_found_running_count++
		}

		if *compare_step_build_step_status_id == *not_started_build_step_status_id {
			incomplete_steps_found = true
			incomplete_steps_found_not_started_count++
		}

		if *compare_step_build_step_status_id == *failed_build_step_status_id {
			incomplete_steps_found_failed_count++
		}

		if *compare_step_build_step_status_id == *passed_build_step_status_id {
			incomplete_steps_found_passed_count++
		}
	}

	if incomplete_steps_found {
		fmt.Println(fmt.Sprintf("there are %d steps with status \"Running\" and %d steps with status \"Not Started\" ignore triggering next step", incomplete_steps_found_running_count, incomplete_steps_found_not_started_count))
		return nil
	}

	if len(errors) == 0 && !incomplete_steps_found {		
		fmt.Println("all previous steps have completed... triggering next step")
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
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