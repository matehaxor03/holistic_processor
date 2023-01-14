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

	one_record := uint64(1)

	lookup_buildstep_records, lookup_buildstep_records_errors := table_BuildStepStatus.ReadRecords(build_step_status_select_array, build_step_status_where_map, nil, nil, &one_record, nil)
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

	table_BuildBranchInstanceStep, table_BuildBranchInstanceStep_errors := database.GetTable("BuildBranchInstanceStep")
	if table_BuildBranchInstanceStep_errors != nil {
		errors = append(errors, table_BuildBranchInstanceStep_errors...)
		return errors
	} else if common.IsNil(table_BuildBranchInstanceStep) {
		errors = append(errors, fmt.Errorf("table_BuildBranchInstanceStep is nil"))
		return errors
	}

	update_records_build_branch_instance_step_data_map :=  map[string]interface{}{"build_branch_instance_step_id":build_branch_instance_step_id}
	update_records_build_branch_instance_step_data :=  json.NewMapOfValues(&update_records_build_branch_instance_step_data_map)

	update_records, update_records_errors := table_BuildBranchInstanceStep.ReadRecords(nil, update_records_build_branch_instance_step_data, nil, nil, &one_record, nil)
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
	update_record.SetUInt64Value("build_step_status_id", *build_step_status_id)
	update_errors := update_record.Update()
	if update_errors != nil {
		errors = append(errors, update_errors...)
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

	next_branch_instance_steps, next_branch_instance_steps_errors := table_BuildBranchInstanceStep.ReadRecords(build_branch_instance_step_select_array, build_branch_instance_step_where_map, build_branch_instance_step_where_logic_map, order_by_array, nil, nil)
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

	next_build_branch_instance_step_select := []string{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order", "parameters"}
	next_build_branch_instance_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&next_build_branch_instance_step_select))

	next_build_branch_instance_step_where := map[string]interface{}{"build_branch_instance_id":*build_branch_instance_id, "order":*new_order}
	next_build_branch_instance_step_where_map := json.NewMapOfValues(&next_build_branch_instance_step_where)

	next_build_branch_instance_step_where_logic := map[string]interface{}{"order":"="}
	next_build_branch_instance_step_where_logic_map := json.NewMapOfValues(&next_build_branch_instance_step_where_logic)
	
	next_branch_instance_steps_array, next_branch_instance_steps_array_errors := table_BuildBranchInstanceStep.ReadRecords(next_build_branch_instance_step_select_array, next_build_branch_instance_step_where_map, next_build_branch_instance_step_where_logic_map, nil, nil, nil)
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

		next_build_steps_where := map[string]interface{}{"build_step_id":*desired_build_step_id}
		next_build_steps_where_map := json.NewMapOfValues(&next_build_steps_where)

		table_BuildStep, table_BuildStep_errors := database.GetTable("BuildStep")
		if table_BuildStep_errors != nil {
			errors = append(errors, table_BuildStep_errors...)
			return errors
		} else if common.IsNil(table_BuildStep) {
			errors = append(errors, fmt.Errorf("table_BuildStep is nil"))
			return errors
		}
		
		lookup_build_step_array, lookup_build_step_array_errors := table_BuildStep.ReadRecords(next_build_steps_select_array, next_build_steps_where_map, nil, nil, nil, nil)
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
		next_build_step_map.SetUInt64("build_branch_id", build_branch_id)

		next_command := map[string]interface{}{"[queue]":*name_of_next_step,"[queue_mode]":"PushBack","[async]":true, "[trace_id]":processor.GenerateTraceId()}
		next_command_map := json.NewMapOfValues(&next_command)
		next_command_map.SetMap("data", next_build_step_map)
	
		go processor.SendMessageToQueueFireAndForget(next_command_map)
	}

	return nil
}