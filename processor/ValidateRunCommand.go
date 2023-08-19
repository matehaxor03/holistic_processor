package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func validateRunCommandHeaders(processor *Processor, request *json.Map) (*string, *uint64, *uint64, *uint64, *uint64, *int64, *string, *string, *string, *string, *string, []error) {
	var errors []error
	one_record := uint64(1)
	write_client := processor.GetClientWrite()
	database := write_client.GetDatabase()

	request_data, request_data_errors := request.GetMap("data")
	if request_data_errors != nil {
		errors = append(errors, request_data_errors...) 
	} else if common.IsNil(request_data) {
		errors = append(errors, fmt.Errorf("request data is nil"))
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	command_name, command_name_errors := request_data.GetString("command_name")
	if command_name_errors != nil {
		errors = append(errors, command_name_errors...) 
	} else if common.IsNil(command_name) {
		errors = append(errors, fmt.Errorf("command_name is nil"))
	}

	branch_instance_step_id, branch_instance_step_id_errors := request_data.GetUInt64("branch_instance_step_id")
	if branch_instance_step_id_errors != nil {
		errors = append(errors, branch_instance_step_id_errors...) 
	} else if common.IsNil(branch_instance_step_id) {
		errors = append(errors, fmt.Errorf("branch_instance_step_id is nil"))
	}

	branch_instance_id, branch_instance_id_errors := request_data.GetUInt64("branch_instance_id")
	if branch_instance_id_errors != nil {
		errors = append(errors, branch_instance_id_errors...) 
	} else if common.IsNil(branch_instance_id) {
		errors = append(errors, fmt.Errorf("branch_instance_id is nil"))
	}

	branch_id, branch_id_errprs := request_data.GetUInt64("branch_id")
	if branch_id_errprs != nil {
		errors = append(errors, branch_id_errprs...) 
	} else if common.IsNil(branch_id) {
		errors = append(errors, fmt.Errorf("branch_id is nil"))
	}

	build_step_id, build_step_id_errors := request_data.GetUInt64("build_step_id")
	if build_step_id_errors != nil {
		errors = append(errors, build_step_id_errors...) 
	} else if common.IsNil(build_step_id) {
		errors = append(errors, fmt.Errorf("build_step_id is nil"))
	}

	order, order_errors := request_data.GetInt64("order")
	if order_errors != nil {
		errors = append(errors, order_errors...) 
	} else if common.IsNil(order) {
		errors = append(errors, fmt.Errorf("build_step_id_errors is nil"))
	}

	domain_name, domain_name_errors := request_data.GetString("domain_name")
	if domain_name_errors != nil {
		errors = append(errors, domain_name_errors...) 
	} else if common.IsNil(domain_name) {
		errors = append(errors, fmt.Errorf("domain_name is nil"))
	}

	repository_account_name, repository_account_name_errors := request_data.GetString("repository_account_name")
	if repository_account_name_errors != nil {
		errors = append(errors, repository_account_name_errors...) 
	} else if common.IsNil(repository_account_name) {
		errors = append(errors, fmt.Errorf("repository_account_name is nil"))
	}

	repository_name, repository_name_errors := request_data.GetString("repository_name")
	if repository_name_errors != nil {
		errors = append(errors, repository_name_errors...) 
	} else if common.IsNil(repository_name) {
		errors = append(errors, fmt.Errorf("repository_name is nil"))
	}

	branch_name, branch_name_errors := request_data.GetString("branch_name")
	if branch_name_errors != nil {
		errors = append(errors, branch_name_errors...) 
	} else if common.IsNil(branch_name) {
		errors = append(errors, fmt.Errorf("branch_name is nil"))
	}	

	parameters, parameters_errors := request_data.GetString("parameters")
	if parameters_errors != nil {
		errors = append(errors, parameters_errors...) 
	} else if common.IsNil(parameters) {
		errors = append(errors, fmt.Errorf("parameters is nil"))
	}	

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	table_BuildStepStatus, table_BuildStepStatus_errors := database.GetTable("BuildStepStatus")
	if table_BuildStepStatus_errors != nil {
		errors = append(errors, table_BuildStepStatus_errors...)
	} else if common.IsNil(table_BuildStepStatus) {
		errors = append(errors, fmt.Errorf("buildstep status table is nil"))
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	where_query_build_step_status_not_started_array := json.NewArray()

	where_query_build_step_status_not_started := json.NewMap()
	where_query_build_step_status_not_started.SetStringValue("column", "name")
	where_query_build_step_status_not_started.SetStringValue("value", "Not Started")
	where_query_build_step_status_not_started.SetStringValue("logic", "=")

	where_query_build_step_status_not_started_array.AppendMap(where_query_build_step_status_not_started)

	records_not_started_step_status, records_not_started_step_status_errors := table_BuildStepStatus.ReadRecords(nil, where_query_build_step_status_not_started_array, nil, nil, nil, nil)
	if records_not_started_step_status_errors != nil {
		errors = append(errors, records_not_started_step_status_errors...)
	} else if len(*records_not_started_step_status) == 0 {
		errors = append(errors, fmt.Errorf("did not find record for Not Started BuildStepStatus"))
	}  else if len(*records_not_started_step_status) > 1 {
		errors = append(errors, fmt.Errorf("found too many records for Not Started BuildStepStatus"))
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	not_started_build_step_status_id, not_started_build_step_status_id_errors := ((*records_not_started_step_status)[0]).GetUInt64("build_step_status_id")
	if not_started_build_step_status_id_errors != nil {
		errors = append(errors, not_started_build_step_status_id_errors...)
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
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
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	running_build_step_status_id, running_build_step_status_id_errors := ((*records_running_step_status)[0]).GetUInt64("build_step_status_id")
	if running_build_step_status_id_errors != nil {
		errors = append(errors, running_build_step_status_id_errors...)
	} else if common.IsNil(running_build_step_status_id) {
		errors = append(errors, fmt.Errorf("validate run command: running_build_step_status_id is nil"))
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	table_BranchInstanceStep, table_BranchInstanceStep_errors := database.GetTable("BranchInstanceStep")
	if table_BranchInstanceStep_errors != nil {
		errors = append(errors, table_BranchInstanceStep_errors...)
	} else if common.IsNil(table_BranchInstanceStep) {
		errors = append(errors, fmt.Errorf("validate run command: table_BranchInstanceStep is nil"))
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	update_records_branch_instance_step_select := []string{"branch_instance_step_id", "build_step_status_id"}
	update_records_branch_instance_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&update_records_branch_instance_step_select))
	
	update_records_branch_instance_step_where_array := json.NewArray()

	update_records_branch_instance_step_where_map := json.NewMap()
	update_records_branch_instance_step_where_map.SetStringValue("column", "branch_instance_step_id")
	update_records_branch_instance_step_where_map.SetUInt64Value("value", *branch_instance_step_id)
	update_records_branch_instance_step_where_map.SetStringValue("logic", "=")

	update_records_branch_instance_step_where_array.AppendMap(update_records_branch_instance_step_where_map)
	
	update_records, update_records_errors := table_BranchInstanceStep.ReadRecords(update_records_branch_instance_step_select_array, update_records_branch_instance_step_where_array, nil, nil, &one_record, nil)
	if update_records_errors != nil {
		errors = append(errors, update_records_errors...)
	} else if common.IsNil(update_records) {
		errors = append(errors, fmt.Errorf("validate run command: update_records is nil"))
	} else if len(*update_records) != 1 {
		errors = append(errors, fmt.Errorf("validate run command: update_records len is not 1"))
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	update_record := (*update_records)[0]
	update_record_build_step_status_id, update_record_build_step_status_id_errors := update_record.GetUInt64("build_step_status_id")
	if update_record_build_step_status_id_errors != nil {
		errors = append(errors, update_record_build_step_status_id_errors...)
	} else if common.IsNil(update_record_build_step_status_id) {
		errors = append(errors, fmt.Errorf("update_record_build_step_status_id is nil"))
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	if *update_record_build_step_status_id == *not_started_build_step_status_id {
		update_record.SetUInt64Value("build_step_status_id", *running_build_step_status_id)
		update_errors := update_record.Update()
		if update_errors != nil {
			errors = append(errors, update_errors...)
		}
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	return command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, nil
}