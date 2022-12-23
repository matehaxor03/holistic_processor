package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func validateRunCommandHeaders(request *json.Map) (*uint64, *uint64, *uint64, *int64, *string, *string, *string, *string, []error) {
	var errors []error

	request_keys := request.Keys()
	request_inner_map, request_inner_map_errors := request.GetMap(request_keys[0])
	if request_inner_map_errors != nil {
		errors = append(errors, request_inner_map_errors...)
	} else if common.IsNil(request_inner_map) {
		errors = append(errors, fmt.Errorf("request inner json is nil"))
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	request_data, request_data_errors := request_inner_map.GetMap("data")
	if request_data_errors != nil {
		errors = append(errors, request_data_errors...) 
	} else if common.IsNil(request_data) {
		errors = append(errors, fmt.Errorf("request data is nil"))
	}

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	build_branch_instance_step_id, build_branch_instance_step_id_errors := request_data.GetUInt64("build_branch_instance_step_id")
	if build_branch_instance_step_id_errors != nil {
		errors = append(errors, build_branch_instance_step_id_errors...) 
	} else if common.IsNil(build_branch_instance_step_id) {
		errors = append(errors, fmt.Errorf("build_branch_instance_step_id is nil"))
	}

	build_branch_instance_id, build_branch_instance_id_errors := request_data.GetUInt64("build_branch_instance_id")
	if build_branch_instance_id_errors != nil {
		errors = append(errors, build_branch_instance_id_errors...) 
	} else if common.IsNil(build_branch_instance_id) {
		errors = append(errors, fmt.Errorf("build_branch_instance_id is nil"))
	}

	build_step_id, build_step_id_errors := request_data.GetUInt64("build_step_id")
	if build_step_id_errors != nil {
		errors = append(errors, build_branch_instance_id_errors...) 
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

	if len(errors) > 0 {
		return nil, nil, nil, nil, nil, nil, nil, nil, errors
	} 

	return build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, nil
}