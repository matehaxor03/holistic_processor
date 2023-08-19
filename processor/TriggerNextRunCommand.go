package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func triggerNextRunCommand(processor *Processor, command_name *string, branch_instance_step_id *uint64, branch_instance_id *uint64, branch_id *uint64, build_step_id *uint64, order *int64, domain_name *string, repository_account_name *string, repository_name *string, branch_name *string, parameters *string, errors []error, request *json.Map) ([]error) {
	if command_name == nil {
		errors = append(errors, fmt.Errorf("current command_name is nil"))
	} 
	one_record := uint64(1)
	write_client := processor.GetClientWrite()
	database := write_client.GetDatabase()
	table_BuildStepStatus, table_BuildStepStatus_errors := database.GetTable("BuildStepStatus")
	if table_BuildStepStatus_errors != nil {
		errors = append(errors, table_BuildStepStatus_errors...)
		return errors
	} else if common.IsNil(table_BuildStepStatus) {
		errors = append(errors, fmt.Errorf("table_BuildStepStatus is nil"))
		return errors
	}

	where_query_build_step_status_not_started_array := json.NewArray()

	where_query_build_step_status_not_started := json.NewMap()
	where_query_build_step_status_not_started.SetStringValue("column", "name")
	where_query_build_step_status_not_started.SetStringValue("value", "Not Started")
	where_query_build_step_status_not_started.SetStringValue("logic", "=")

	where_query_build_step_status_not_started_array.AppendMap(where_query_build_step_status_not_started)

	records_not_started_step_status, records_not_started_step_status_errors := table_BuildStepStatus.ReadRecords(nil, where_query_build_step_status_not_started_array, nil, nil, nil, nil)
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

	where_query_build_step_status_running_array := json.NewArray()

	where_query_build_step_status_running := json.NewMap()
	where_query_build_step_status_running.SetStringValue("column", "name")
	where_query_build_step_status_running.SetStringValue("value", "Running")
	where_query_build_step_status_running.SetStringValue("logic", "=")

	where_query_build_step_status_running_array.AppendMap(where_query_build_step_status_running)

	records_running_step_status, records_running_step_status_errors := table_BuildStepStatus.ReadRecords(nil, where_query_build_step_status_running_array, nil, nil, nil, nil)
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

	table_BranchInstanceStep, table_BranchInstanceStep_errors := database.GetTable("BranchInstanceStep")
	if table_BranchInstanceStep_errors != nil {
		errors = append(errors, table_BranchInstanceStep_errors...)
		return errors
	} else if common.IsNil(table_BranchInstanceStep) {
		errors = append(errors, fmt.Errorf("table_BranchInstanceStep is nil"))
		return errors
	}

	update_records_build_branch_instance_step_select := []string{"branch_instance_step_id", "build_step_status_id"}
	update_records_build_branch_instance_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&update_records_build_branch_instance_step_select))
	
	update_records_build_branch_instance_step_where_array := json.NewArray()
	
	update_records_build_branch_instance_step_where_map := json.NewMap()
	update_records_build_branch_instance_step_where_map.SetStringValue("column", "branch_instance_step_id")
	update_records_build_branch_instance_step_where_map.SetUInt64Value("value", *branch_instance_step_id)
	update_records_build_branch_instance_step_where_map.SetStringValue("logic", "=")

	update_records_build_branch_instance_step_where_array.AppendMap(update_records_build_branch_instance_step_where_map)
	
	update_records, update_records_errors := table_BranchInstanceStep.ReadRecords(update_records_build_branch_instance_step_select_array, update_records_build_branch_instance_step_where_array, nil, nil, &one_record, nil)
	if update_records_errors != nil {
		errors = append(errors, update_records_errors...)
		return errors
	} else if common.IsNil(update_records) {
		errors = append(errors, fmt.Errorf("update_records is nil"))
		return errors
	} else if len(*update_records) != 1 {
		errors = append(errors, fmt.Errorf("update_records len is not 1"))
		return errors
	}
	
	update_record := (*update_records)[0]
	current_build_step_status_id, current_build_step_status_id_errors := update_record.GetUInt64("build_step_status_id")
	if current_build_step_status_id_errors != nil {
		errors = append(errors, current_build_step_status_id_errors...)
		return errors
	} else if common.IsNil(current_build_step_status_id) {
		errors = append(errors, fmt.Errorf("current_build_step_status_id is nil"))
		return errors
	}

	if !(*current_build_step_status_id == *running_build_step_status_id ||
	   *current_build_step_status_id == *not_started_build_step_status_id) {
		fmt.Println("already synced")
		return nil
	}


	lookup_name := ""
	if len(errors) > 0 {
		lookup_name = "Failed"
	} else {
		lookup_name = "Passed"
	}

	build_step_status_select := []string{"build_step_status_id"}
	build_step_status_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&build_step_status_select))

	build_step_status_where_array := json.NewArray()
	
	build_step_status_where_map := json.NewMap()
	build_step_status_where_map.SetStringValue("column", "name")
	build_step_status_where_map.SetStringValue("value", lookup_name)
	build_step_status_where_map.SetStringValue("logic", "=")

	build_step_status_where_array.AppendMap(build_step_status_where_map)

	lookup_buildstep_records, lookup_buildstep_records_errors := table_BuildStepStatus.ReadRecords(build_step_status_select_array, build_step_status_where_array, nil, nil, &one_record, nil)
	if lookup_buildstep_records_errors != nil {
		errors = append(errors, lookup_buildstep_records_errors...)
		return errors
	} else if common.IsNil(lookup_buildstep_records) {
		errors = append(errors, fmt.Errorf("lookup_buildstep_records is nil"))
		return errors
	} else if len(*lookup_buildstep_records) != 1 {
		errors = append(errors, fmt.Errorf("lookup_buildstep_records did not return 1 record"))
		return errors
	}

	build_step_status :=  (*lookup_buildstep_records)[0]
	build_step_status_id, build_step_status_id_errors := build_step_status.GetUInt64("build_step_status_id")
	if build_step_status_id_errors != nil {
		errors = append(errors, build_step_status_id_errors...)
		return errors
	} else if common.IsNil(build_step_status_id) {
		errors = append(errors, fmt.Errorf("build_step_status_id is nil"))
		return errors
	}

	update_record.SetUInt64Value("build_step_status_id", *build_step_status_id)
	update_errors := update_record.Update()
	if update_errors != nil {
		errors = append(errors, update_errors...)
		return errors
	} 

	branch_instance_step_select := []string{"branch_instance_step_id", "branch_instance_id", "build_step_id", "order"}
	branch_instance_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&branch_instance_step_select))

	branch_instance_step_where_array := json.NewArray()

	branch_instance_step_where_map_branch_instance_id := json.NewMap()
	branch_instance_step_where_map_branch_instance_id.SetStringValue("column", "branch_instance_id")
	branch_instance_step_where_map_branch_instance_id.SetUInt64Value("value", *branch_instance_id)
	branch_instance_step_where_map_branch_instance_id.SetStringValue("logic", "=")

	branch_instance_step_where_map_order_id := json.NewMap()
	branch_instance_step_where_map_order_id.SetStringValue("column", "order")
	branch_instance_step_where_map_order_id.SetInt64Value("value", *order)
	branch_instance_step_where_map_order_id.SetStringValue("logic", ">")

	branch_instance_step_where_array.AppendMap(branch_instance_step_where_map_branch_instance_id)
	branch_instance_step_where_array.AppendMap(branch_instance_step_where_map_order_id)

	branch_instance_step_order_by := map[string]interface{}{"order":"ascending"}
	branch_instance_step_order_by_map := json.NewMapOfValues(&branch_instance_step_order_by)
	order_by_array := json.NewArray()
	order_by_array.AppendMap(branch_instance_step_order_by_map)

	next_branch_instance_steps, next_branch_instance_steps_errors := table_BranchInstanceStep.ReadRecords(branch_instance_step_select_array, branch_instance_step_where_array, nil, order_by_array, nil, nil)
	if next_branch_instance_steps_errors != nil {
		errors = append(errors, next_branch_instance_steps_errors...)
		return errors
	} else if common.IsNil(next_branch_instance_steps) {
		errors = append(errors, fmt.Errorf("next_branch_instance_steps is nil"))
		return errors
	}

	if len(*next_branch_instance_steps) == 0 {
		return nil
	}

	next_branch_instance_build_step :=  (*next_branch_instance_steps)[0]
	new_order, new_order_errors := next_branch_instance_build_step.GetInt64("order")
	if new_order_errors != nil {
		errors = append(errors, new_order_errors...)
	} else if common.IsNil(new_order) {
		errors = append(errors, fmt.Errorf("new_order is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	next_build_branch_instance_step_select := []string{"branch_instance_step_id", "branch_instance_id", "build_step_id", "order", "parameters"}
	next_build_branch_instance_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&next_build_branch_instance_step_select))

	next_build_branch_instance_step_where_array := json.NewArray()

	next_build_branch_instance_step_where_branch_instance_id := json.NewMap()
	next_build_branch_instance_step_where_branch_instance_id.SetStringValue("column", "branch_instance_id")
	next_build_branch_instance_step_where_branch_instance_id.SetUInt64Value("value", *branch_instance_id)
	next_build_branch_instance_step_where_branch_instance_id.SetStringValue("logic", "=")

	next_build_branch_instance_step_where_order := json.NewMap()
	next_build_branch_instance_step_where_order.SetStringValue("column", "order")
	next_build_branch_instance_step_where_order.SetInt64Value("value", *new_order)
	next_build_branch_instance_step_where_order.SetStringValue("logic", "=")

	next_build_branch_instance_step_where_array.AppendMap(next_build_branch_instance_step_where_branch_instance_id)
	next_build_branch_instance_step_where_array.AppendMap(next_build_branch_instance_step_where_order)
	
	next_branch_instance_steps_array, next_branch_instance_steps_array_errors := table_BranchInstanceStep.ReadRecords(next_build_branch_instance_step_select_array, next_build_branch_instance_step_where_array, nil, nil, nil, nil)
	if next_branch_instance_steps_array_errors != nil {
		errors = append(errors, next_branch_instance_steps_array_errors...)
	} else if common.IsNil(next_branch_instance_steps_array) {
		errors = append(errors, fmt.Errorf("next_branch_instance_steps_array is nil"))
	}
	
	for _, next_branch_instance_step := range *next_branch_instance_steps_array {
		next_build_step := next_branch_instance_step
		desired_build_step_id, desired_build_step_id_errors := next_build_step.GetUInt64("build_step_id")
		if desired_build_step_id_errors != nil {
			errors = append(errors, desired_build_step_id_errors...)
		} else if common.IsNil(desired_build_step_id) {
			errors = append(errors, fmt.Errorf("build_step_id is nil"))
			return errors
		}

		next_build_steps_select := []string{"name"}
		next_build_steps_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&next_build_steps_select))

		next_build_steps_where_array := json.NewArray()
		
		next_build_steps_where_map := json.NewMap()
		next_build_steps_where_map.SetStringValue("column", "build_step_id")
		next_build_steps_where_map.SetUInt64Value("value", *desired_build_step_id)
		next_build_steps_where_map.SetStringValue("logic", "=")

		next_build_steps_where_array.AppendMap(next_build_steps_where_map)
		
		table_BuildStep, table_BuildStep_errors := database.GetTable("BuildStep")
		if table_BuildStep_errors != nil {
			errors = append(errors, table_BuildStep_errors...)
			return errors
		} else if common.IsNil(table_BuildStep) {
			errors = append(errors, fmt.Errorf("table_BuildStep is nil"))
			return errors
		}
		
		lookup_build_step_array, lookup_build_step_array_errors := table_BuildStep.ReadRecords(next_build_steps_select_array, next_build_steps_where_array, nil, nil, nil, nil)
		if lookup_build_step_array_errors != nil {
			errors = append(errors, lookup_build_step_array_errors...)
			return errors
		} else if common.IsNil(lookup_build_step_array) {
			errors = append(errors, fmt.Errorf("lookup_build_step_array is nil"))
			return errors
		} else if len(*lookup_build_step_array) != 1 {
			errors = append(errors, fmt.Errorf("lookup_build_step_array did not have 1 record"))
			return errors
		}

		build_step := ((*lookup_build_step_array))[0]
		name_of_next_step, name_of_next_step_errors := build_step.GetString("name")
		if name_of_next_step_errors != nil {
			errors = append(errors, name_of_next_step_errors...)
			return errors
		} else if common.IsNil(name_of_next_step) {
			errors = append(errors, fmt.Errorf("name attribute for next step is nil"))
			return errors
		}

		next_build_step_map, next_build_step_map_errors := next_build_step.GetFields()
		if next_build_step_map_errors != nil {
			errors = append(errors, next_build_step_map_errors...)
			return errors
		} else if common.IsNil(next_build_step_map) {
			errors = append(errors, fmt.Errorf("next_build_step_map is nil"))
			return errors
		}

		next_build_step_map.SetString("domain_name", domain_name)
		next_build_step_map.SetString("repository_account_name", repository_account_name)
		next_build_step_map.SetString("repository_name", repository_name)
		next_build_step_map.SetString("branch_name", branch_name)
		next_build_step_map.SetString("command_name", name_of_next_step)
		next_build_step_map.SetUInt64("branch_id", branch_id)

		next_command := map[string]interface{}{"[queue]":*name_of_next_step,"[queue_mode]":"PushBack","[async]":true, "[trace_id]":processor.GenerateTraceId()}
		next_command_map := json.NewMapOfValues(&next_command)
		next_command_map.SetMap("data", next_build_step_map)
	
		go processor.SendMessageToQueueFireAndForget(next_command_map)
	}

	return nil
}