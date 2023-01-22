package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func commandRunEnd(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors := validateRunCommandHeaders(processor, request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} 

	one_record := uint64(1)
	write_client := processor.GetClientWrite()
	database := write_client.GetDatabase()

	table_BuildStepStatus, table_BuildStepStatus_errors := database.GetTable("BuildStepStatus")
	if table_BuildStepStatus_errors != nil {
		errors = append(errors, table_BuildStepStatus_errors...)
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	} else if common.IsNil(table_BuildStepStatus) {
		errors = append(errors, fmt.Errorf("buildstep status table is nil"))
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
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

	lookup_buildstep_records, lookup_buildstep_records_errors := table_BuildStepStatus.ReadRecords(build_step_status_select_array, build_step_status_where_map, nil, nil, nil, &one_record, nil)
	if lookup_buildstep_records_errors != nil {
		errors = append(errors, lookup_buildstep_records_errors...)
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	} else if common.IsNil(lookup_buildstep_records) {
		errors = append(errors, fmt.Errorf("lookup_buildstep_records is nil"))
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	} else if len(*lookup_buildstep_records) != 1 {
		errors = append(errors, fmt.Errorf("lookup_buildstep_records did not return 1 record"))
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	}

	build_step_status :=  (*lookup_buildstep_records)[0]
	build_step_status_id, build_step_status_id_errors := build_step_status.GetUInt64("build_step_status_id")
	if build_step_status_id_errors != nil {
		errors = append(errors, build_step_status_id_errors...)
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	} else if common.IsNil(build_step_status_id) {
		errors = append(errors, fmt.Errorf("build_step_status_id is nil"))
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	}

	table_BranchInstance, table_BranchInstance_errors := database.GetTable("BranchInstance")
	if table_BranchInstance_errors != nil {
		errors = append(errors, table_BranchInstance_errors...)
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	} else if common.IsNil(table_BranchInstance) {
		errors = append(errors, fmt.Errorf("run end: table_BranchInstance is nil"))
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	}

	update_records_branch_instance_select := []string{"branch_instance_id", "build_step_status_id"}
	update_records_branch_instance_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&update_records_branch_instance_select))
	update_records_branch_instance_where :=  map[string]interface{}{"branch_instance_id":*branch_instance_id}
	update_records_branch_instance_where_map :=  json.NewMapOfValues(&update_records_branch_instance_where)

	update_records, update_records_errors := table_BranchInstance.ReadRecords(update_records_branch_instance_select_array, update_records_branch_instance_where_map, nil, nil, nil, &one_record, nil)
	if update_records_errors != nil {
		errors = append(errors, update_records_errors...)
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	} else if common.IsNil(update_records) {
		errors = append(errors, fmt.Errorf("run end: update_records is nil"))
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	} else if len(*update_records) != 1 {
		errors = append(errors, fmt.Errorf("run end: update_records len is not 1"))
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	}

	update_record := (*update_records)[0]
	update_record.SetUInt64Value("build_step_status_id", *build_step_status_id)
	update_errors := update_record.Update()
	if update_errors != nil {
		errors = append(errors, update_errors...)
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			return trigger_next_run_command_errors
		}
		return errors
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunEndFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunEnd
	return &funcValue
}