package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func commandRunNotStarted(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors := validateRunCommandHeaders(processor, request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} else if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}
	
	one_record := uint64(1)
	write_client := processor.GetClientWrite()
	database := write_client.GetDatabase()

	table_BuildStepStatus, table_BuildStepStatus_errors := database.GetTable("BuildStepStatus")
	if table_BuildStepStatus_errors != nil {
		errors = append(errors, table_BuildStepStatus_errors...)
	} else if common.IsNil(table_BuildStepStatus) {
		errors = append(errors, fmt.Errorf("buildstep status table is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 

	where_query_build_step_status_running_array := json.NewArray()

	where_query_build_step_status_running := json.NewMap()
	where_query_build_step_status_running.SetStringValue("column", "name")
	where_query_build_step_status_running.SetStringValue("value", "Running")
	where_query_build_step_status_running.SetStringValue("logic", "=")

	where_query_build_step_status_running_array.AppendMap(where_query_build_step_status_running)

	records_running_step_status, records_running_step_status_errors := table_BuildStepStatus.ReadRecords(nil, where_query_build_step_status_running_array, nil, nil, nil, nil)
	if records_running_step_status_errors != nil {
		errors = append(errors, records_running_step_status_errors...)
	} else if len(*records_running_step_status) == 0 {
		errors = append(errors, fmt.Errorf("validate run command: did not find record for Running BuildStepStatus"))
	}  else if len(*records_running_step_status) > 1 {
		errors = append(errors, fmt.Errorf("validate run command: found too many records for Running BuildStepStatus"))
	}

	if len(errors) > 0 {
		return errors
	} 

	running_build_step_status_id, running_build_step_status_id_errors := ((*records_running_step_status)[0]).GetUInt64("build_step_status_id")
	if running_build_step_status_id_errors != nil {
		errors = append(errors, running_build_step_status_id_errors...)
	} else if common.IsNil(running_build_step_status_id) {
		errors = append(errors, fmt.Errorf("run not started: running_build_step_status_id is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 

	table_BranchInstance, table_BranchInstance_errors := database.GetTable("BranchInstance")
	if table_BranchInstance_errors != nil {
		errors = append(errors, table_BranchInstance_errors...)
	} else if common.IsNil(table_BranchInstance) {
		errors = append(errors, fmt.Errorf("not started: table_BranchInstance is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 

	update_records_branch_instance_select := []string{"branch_instance_id", "build_step_status_id"}
	update_records_branch_instance_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&update_records_branch_instance_select))
	
	update_records_branch_instance_where_array := json.NewArray()

	update_records_branch_instance_where_map := json.NewMap()
	update_records_branch_instance_where_map.SetStringValue("column", "branch_instance_id")
	update_records_branch_instance_where_map.SetUInt64Value("value", *branch_instance_id)
	update_records_branch_instance_where_map.SetStringValue("logic", "=")

	update_records_branch_instance_where_array.AppendMap(update_records_branch_instance_where_map)

	update_records, update_records_errors := table_BranchInstance.ReadRecords(update_records_branch_instance_select_array, update_records_branch_instance_where_array, nil, nil, &one_record, nil)
	if update_records_errors != nil {
		errors = append(errors, update_records_errors...)
	} else if common.IsNil(update_records) {
		errors = append(errors, fmt.Errorf("not started: update_records is nil"))
	} else if len(*update_records) != 1 {
		errors = append(errors, fmt.Errorf("not started: update_records len is not 1"))
	}

	if len(errors) > 0 {
		return errors
	} 

	update_record := (*update_records)[0]
	update_record.SetUInt64Value("build_step_status_id", *running_build_step_status_id)
	update_errors := update_record.Update()
	if update_errors != nil {
		errors = append(errors, update_errors...)
	}

	if len(errors) > 0 {
		return errors
	} 

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunNotStartedFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunNotStarted
	return &funcValue
}