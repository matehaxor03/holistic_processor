package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func commandRunStartBuildBranchInstance(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error

	request_data, request_data_errors := request.GetMap("data")
	if request_data_errors != nil {
		errors = append(errors, request_data_errors...) 
	} else if common.IsNil(request_data) {
		errors = append(errors, fmt.Errorf("request data is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 

	build_branch_instance_id, build_branch_instance_id_errors := request_data.GetUInt64("build_branch_instance_id")
	if build_branch_instance_id_errors != nil {
		errors = append(errors, build_branch_instance_id_errors...) 
	} else if common.IsNil(build_branch_instance_id) {
		errors = append(errors, fmt.Errorf("build_branch_instance_id is nil"))
	}

	build_branch_id, build_branch_id_errors := request_data.GetUInt64("build_branch_id")
	if build_branch_id_errors != nil {
		errors = append(errors, build_branch_id_errors...) 
	} else if common.IsNil(build_branch_id) {
		errors = append(errors, fmt.Errorf("build_branch_id is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 

	
	all_build_steps_order_by :=  map[string]interface{}{"order":"ascending"}
	all_build_steps_order_by_map := json.NewMapOfValues(&all_build_steps_order_by)
	all_build_steps_order_by_array := json.NewArray()
	all_build_steps_order_by_array.AppendMap(all_build_steps_order_by_map)
	all_build_steps_request_map := map[string]interface{}{"[queue]":"ReadRecords_BuildStep", "[trace_id]":processor.GenerateTraceId()}
	all_build_steps_request := json.NewMapOfValues(&all_build_steps_request_map)
	all_build_steps_request.SetArray("[order_by]", all_build_steps_order_by_array)
	all_build_steps_response, all_build_steps_response_errors := processor.SendMessageToQueue(all_build_steps_request)
	if all_build_steps_response_errors != nil {
		errors = append(errors, all_build_steps_response_errors...)
	} else if common.IsNil(all_build_steps_response) {
		errors = append(errors, fmt.Errorf("response is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 
	

	if !all_build_steps_response.HasKey("data") {
		errors = append(errors, fmt.Errorf("data not found"))
	} else if !all_build_steps_response.IsArray("data") {
		errors = append(errors, fmt.Errorf("data is not an array"))
	} 

	if len(errors) > 0 {
		return errors
	}

	build_steps, build_steps_array := all_build_steps_response.GetArray("data")
	if build_steps_array != nil {
		errors = append(errors, build_steps_array...)
	} else if common.IsNil(build_steps) {
		errors = append(errors, fmt.Errorf("build steps is nil"))
	} else if len(*(build_steps.GetValues())) == 0 {
		errors = append(errors, fmt.Errorf("no build steps were found"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_branch_instance_steps := json.NewArray()
	for _, build_step_interface := range *(build_steps.GetValues()) {
		current_build_step, current_build_step_errors := build_step_interface.GetMap()
		if current_build_step_errors != nil {
			errors = append(errors, current_build_step_errors...)
		} else if common.IsNil(current_build_step) {
			errors = append(errors, fmt.Errorf("current build step is nil"))
		}

		if len(errors) > 0 {
			return errors
		}

		build_step_id, build_step_id_errors := current_build_step.GetUInt64("build_step_id")
		if build_step_id_errors != nil {
			errors = append(errors, build_step_id_errors...)
		} else if common.IsNil(build_step_id) {
			errors = append(errors, fmt.Errorf("build_step_id attribute is nil"))
		}

		if len(errors) > 0 {
			return errors
		}

		order, order_errors := current_build_step.GetInt64("order")
		if order_errors != nil {
			errors = append(errors, order_errors...)
		} else if common.IsNil(order) {
			errors = append(errors, fmt.Errorf("order attribute is nil"))
		}

		if len(errors) > 0 {
			return errors
		}

		name, name_errors := current_build_step.GetString("name") 
		if name_errors != nil {
			errors = append(errors, name_errors...)
		} else if common.IsNil(name) {
			errors = append(errors, fmt.Errorf("name attribute is nil"))
		}

		if len(errors) > 0 {
			return errors
		}

		//determine this later
		if *name == "Run_IntegrationTestSuite" {
			continue
		}

		build_branch_instance_step :=  map[string]interface{}{"build_branch_instance_id":*build_branch_instance_id, "build_step_id":*build_step_id, "order":*order}
		build_branch_instance_steps.AppendMap(json.NewMapOfValues(&build_branch_instance_step))
	}
	

	create_instance_steps_request_map := map[string]interface{}{"[queue]":"CreateRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId()}
	create_instance_steps_request := json.NewMapOfValues(&create_instance_steps_request_map)
	create_instance_steps_request.SetArray("data", build_branch_instance_steps)
	create_instance_steps_response, create_instance_steps_response_errors := processor.SendMessageToQueue(create_instance_steps_request)
	if create_instance_steps_response_errors != nil {
		errors = append(errors, create_instance_steps_response_errors...)
	} else if common.IsNil(create_instance_steps_response) {
		errors = append(errors, fmt.Errorf("create_instance_steps_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}


	read_records_build_branch_instance_step_select := []string{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order"}
	read_records_build_branch_instance_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&read_records_build_branch_instance_step_select))

	read_records_build_branch_instance_step_where := map[string]interface{}{"build_branch_instance_id":*build_branch_instance_id}
	read_records_build_branch_instance_step_where_map := json.NewMapOfValues(&read_records_build_branch_instance_step_where)

	read_records_build_branch_instance_step_where_order_by :=  map[string]interface{}{"order":"ascending"}
	read_records_build_branch_instance_step_where_order_by_map := json.NewMapOfValues(&read_records_build_branch_instance_step_where_order_by)
	read_records_build_branch_instance_step_where_order_by_array := json.NewArray()
	read_records_build_branch_instance_step_where_order_by_array.AppendMap(read_records_build_branch_instance_step_where_order_by_map)

	read_records_build_branch_instance_step_request := map[string]interface{}{"[queue]":"ReadRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	read_records_build_branch_instance_step_request_map := json.NewMapOfValues(&read_records_build_branch_instance_step_request)
	read_records_build_branch_instance_step_request_map.SetArray("[select_fields]", read_records_build_branch_instance_step_select_array)
	read_records_build_branch_instance_step_request_map.SetMap("[where_fields]", read_records_build_branch_instance_step_where_map)
	read_records_build_branch_instance_step_request_map.SetArray("[order_by]", read_records_build_branch_instance_step_where_order_by_array)

	read_records_build_branch_instance_step_response, read_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(read_records_build_branch_instance_step_request_map)
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
	} else if len(*(first_build_step_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("first_build_step_array does not have one element"))

	}

	if len(errors) > 0 {
		return errors
	}
	
	first_build_step, first_build_step_errors := (*(first_build_step_array.GetValues()))[0].GetMap()
	if first_build_step_errors != nil {
		errors = append(errors, first_build_step_errors...)
	} else if common.IsNil(first_build_step) {
		errors = append(errors, fmt.Errorf("first_build_step is nil"))
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

	read_records_build_step_select := []string{"name"}
	read_records_build_step_select_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&read_records_build_step_select))

	read_records_build_step_where := map[string]interface{}{"build_step_id":*desired_build_step_id}
	read_records_build_step_where_map := json.NewMapOfValues(&read_records_build_step_where)

	read_records_build_step_request := map[string]interface{}{"[queue]":"ReadRecords_BuildStep", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	read_records_build_step_request_map := json.NewMapOfValues(&read_records_build_step_request)
	read_records_build_step_request_map.SetArray("[select_fields]", read_records_build_step_select_array)
	read_records_build_step_request_map.SetMap("[where_fields]", read_records_build_step_where_map)

	read_records_build_step_response, read_records_build_step_response_errors := processor.SendMessageToQueue(read_records_build_step_request_map)
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
	} else if len(*(lookup_build_step_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("lookup_build_step_array does not have one element"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_step, build_step_errors := (*(lookup_build_step_array.GetValues()))[0].GetMap()
	if build_step_errors != nil {
		errors = append(errors, build_step_errors...)
	} else if common.IsNil(build_step) {
		errors = append(errors, fmt.Errorf("build_step is nil"))
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

	read_records_build_branch_select := []string{"build_id", "branch_id"}
	read_records_build_branch_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&read_records_build_branch_select))

	read_records_build_branch_where := map[string]interface{}{"build_branch_id":*build_branch_id}
	read_records_build_branch_where_map := json.NewMapOfValues(&read_records_build_branch_where)

	read_records_build_branch_request := map[string]interface{}{"[queue]":"ReadRecords_BuildBranch", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	read_records_build_branch_request_map := json.NewMapOfValues(&read_records_build_branch_request)
	read_records_build_branch_request_map.SetArray("[select_fields]", read_records_build_branch_array)
	read_records_build_branch_request_map.SetMap("[where_fields]",read_records_build_branch_where_map)

	build_branch_records_response, build_branch_records_response_errors := processor.SendMessageToQueue(read_records_build_branch_request_map)
	if build_branch_records_response_errors != nil {
		errors = append(errors, build_branch_records_response_errors...)
	} else if  common.IsNil(build_branch_records_response) {
		errors = append(errors, fmt.Errorf("build_branch_records_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_branch_records_data_array, build_branch_records_data_array_errors := build_branch_records_response.GetArray("data")
	if build_branch_records_data_array_errors != nil {
		errors = append(errors, build_branch_records_data_array_errors...)
	} else if  common.IsNil(build_branch_records_data_array) {
		errors = append(errors, fmt.Errorf("build_branch_records_data_array is nil"))
	} else if len(*(build_branch_records_data_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("build_branch_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_branch, build_branch_errors := (*(build_branch_records_data_array.GetValues()))[0].GetMap()
	if build_branch_errors != nil {
		errors = append(errors, build_branch_errors...)
	} else if common.IsNil(build_branch) {
		errors = append(errors, fmt.Errorf("build_branch is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	branch_id, branch_id_errors := build_branch.GetUInt64("branch_id")
	if branch_id_errors != nil {
		errors = append(errors, branch_id_errors...)
	} else if  common.IsNil(branch_id) {
		errors = append(errors, fmt.Errorf("branch_id is nil"))
	}

	build_id, build_id_errors := build_branch.GetUInt64("build_id")
	if build_id_errors != nil {
		errors = append(errors, build_id_errors...)
	} else if  common.IsNil(build_id) {
		errors = append(errors, fmt.Errorf("build_id is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_branch_select := []string{"name"}
	read_records_branch_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&read_records_branch_select))

	read_records_branch_where := map[string]interface{}{"branch_id":*branch_id}
	read_records_branch_where_map := json.NewMapOfValues(&read_records_branch_where)

	read_records_branch_request := map[string]interface{}{"[queue]":"ReadRecords_Branch", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	read_records_branch_request_map := json.NewMapOfValues(&read_records_branch_request)
	read_records_branch_request_map.SetArray("[select_fields]", read_records_branch_array)
	read_records_branch_request_map.SetMap("[where_fields]",read_records_branch_where_map)

	read_records_branch_response, read_records_branch_response_errors := processor.SendMessageToQueue(read_records_branch_request_map)
	if read_records_branch_response_errors != nil {
		errors = append(errors, read_records_branch_response_errors...)
	} else if  common.IsNil(read_records_branch_response) {
		errors = append(errors, fmt.Errorf("read_records_branch_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	branch_records_data_array, branch_records_data_array_errors := read_records_branch_response.GetArray("data")
	if branch_records_data_array_errors != nil {
		errors = append(errors, branch_records_data_array_errors...)
	} else if common.IsNil(branch_records_data_array) {
		errors = append(errors, fmt.Errorf("branch_records_data_array is nil"))
	} else if len(*(branch_records_data_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("branch_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	branch, branch_errors := (*(branch_records_data_array.GetValues()))[0].GetMap()
	if branch_errors != nil {
		errors = append(errors, branch_errors...)
	} else if  common.IsNil(branch) {
		errors = append(errors, fmt.Errorf("branch is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	branch_name, branch_name_errors := branch.GetString("name")
	if branch_name_errors != nil {
		errors = append(errors, branch_name_errors...)
	} else if  common.IsNil(branch_name) {
		errors = append(errors, fmt.Errorf("branch_name is nil"))
	}


	read_records_build_select := []string{"domain_name_id", "repository_account_id", "repository_id"}
	read_records_build_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&read_records_build_select))

	read_records_build_where := map[string]interface{}{"build_id":*build_id}
	read_records_build_where_map := json.NewMapOfValues(&read_records_build_where)

	read_records_build_request := map[string]interface{}{"[queue]":"ReadRecords_Build", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	read_records_build_request_map := json.NewMapOfValues(&read_records_build_request)
	read_records_build_request_map.SetArray("[select_fields]", read_records_build_array)
	read_records_build_request_map.SetMap("[where_fields]",read_records_build_where_map)

	read_records_build_response, read_records_build_response_errors := processor.SendMessageToQueue(read_records_build_request_map)
	if read_records_build_response_errors != nil {
		errors = append(errors, read_records_build_response_errors...)
	} else if  common.IsNil(read_records_branch_response) {
		errors = append(errors, fmt.Errorf("read_records_build_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_records_data_array, build_records_data_array_errors := read_records_build_response.GetArray("data")
	if build_records_data_array_errors != nil {
		errors = append(errors, build_records_data_array_errors...)
	} else if  common.IsNil(build_records_data_array) {
		errors = append(errors, fmt.Errorf("build_records_data_array is nil"))
	} else if len(*(build_records_data_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("build_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	build, build_errors := (*(build_records_data_array.GetValues()))[0].GetMap()
	if build_errors != nil {
		errors = append(errors, build_errors...)
	} else if  common.IsNil(build) {
		errors = append(errors, fmt.Errorf("build is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	domain_name_id, domain_name_id_errors := build.GetUInt64("domain_name_id")
	if domain_name_id_errors != nil {
		errors = append(errors, domain_name_id_errors...)
	} else if  common.IsNil(domain_name_id) {
		errors = append(errors, fmt.Errorf("domain_name_id is nil"))
	}

	repository_account_id, repository_account_id_errors := build.GetUInt64("repository_account_id")
	if repository_account_id_errors != nil {
		errors = append(errors, repository_account_id_errors...)
	} else if  common.IsNil(repository_account_id) {
		errors = append(errors, fmt.Errorf("repository_account_id is nil"))
	}

	repository_id, repository_id_errors := build.GetUInt64("repository_id")
	if repository_id_errors != nil {
		errors = append(errors, repository_id_errors...)
	} else if  common.IsNil(repository_id) {
		errors = append(errors, fmt.Errorf("repository_id is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_domain_name_select := []string{"name"}
	read_records_domain_name_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&read_records_domain_name_select))

	read_records_domain_name_where := map[string]interface{}{"domain_name_id":*domain_name_id}
	read_records_domain_name_where_map := json.NewMapOfValues(&read_records_domain_name_where)

	read_records_domain_name_request := map[string]interface{}{"[queue]":"ReadRecords_DomainName", "[trace_id]":processor.GenerateTraceId(),"[limit]":1}
	read_records_domain_name_request_map := json.NewMapOfValues(&read_records_domain_name_request)
	read_records_domain_name_request_map.SetArray("[select_fields]", read_records_domain_name_array)
	read_records_domain_name_request_map.SetMap("[where_fields]",read_records_domain_name_where_map)

	read_records_domain_name_response, read_records_domain_name_response_errors := processor.SendMessageToQueue(read_records_domain_name_request_map)
	if read_records_domain_name_response_errors != nil {
		errors = append(errors, read_records_domain_name_response_errors...)
	} else if  common.IsNil(read_records_domain_name_response) {
		errors = append(errors, fmt.Errorf("read_records_domain_name_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	domain_name_records_data_array, domain_name_records_data_array_errors := read_records_domain_name_response.GetArray("data")
	if domain_name_records_data_array_errors != nil {
		errors = append(errors, domain_name_records_data_array_errors...)
	} else if  common.IsNil(domain_name_records_data_array) {
		errors = append(errors, fmt.Errorf("domain_name_records_data_array is nil"))
	} else if len(*(domain_name_records_data_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("domain_name_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	domain_name_map, domain_name_map_errors := (*(domain_name_records_data_array.GetValues()))[0].GetMap()
	if domain_name_map_errors != nil {
		errors = append(errors, domain_name_map_errors...)
	} else if  common.IsNil(domain_name_map) {
		errors = append(errors, fmt.Errorf("domain_name_map is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	domain_name, domain_name_errors := domain_name_map.GetString("name")
	if domain_name_errors != nil {
		errors = append(errors, domain_name_errors...)
	} else if  common.IsNil(domain_name) {
		errors = append(errors, fmt.Errorf("domain_name is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_repository_account_name_select := []string{"name"}
	read_records_repository_account_name_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&read_records_repository_account_name_select))

	read_records_repository_account_name_where := map[string]interface{}{"repository_account_id":*repository_account_id}
	read_records_repository_account_name_where_map := json.NewMapOfValues(&read_records_repository_account_name_where)

	read_records_repository_account_name_request := map[string]interface{}{"[queue]":"ReadRecords_RepositoryAccount", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	read_records_repository_account_name_request_map := json.NewMapOfValues(&read_records_repository_account_name_request)
	read_records_repository_account_name_request_map.SetArray("[select_fields]", read_records_repository_account_name_array)
	read_records_repository_account_name_request_map.SetMap("[where_fields]", read_records_repository_account_name_where_map)

	read_records_repository_account_name_response, read_records_repository_account_name_response_errors := processor.SendMessageToQueue(read_records_repository_account_name_request_map)
	if read_records_repository_account_name_response_errors != nil {
		errors = append(errors, read_records_repository_account_name_response_errors...)
	} else if  common.IsNil(read_records_domain_name_response) {
		errors = append(errors, fmt.Errorf("read_records_repository_account_name_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	repository_account_name_records_data_array, repository_account_name_records_data_array_errors := read_records_repository_account_name_response.GetArray("data")
	if repository_account_name_records_data_array_errors != nil {
		errors = append(errors, repository_account_name_records_data_array_errors...)
	} else if  common.IsNil(repository_account_name_records_data_array) {
		errors = append(errors, fmt.Errorf("repository_account_name_records_data_array is nil"))
	} else if len(*(repository_account_name_records_data_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("repository_account_name_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	repository_account_name_map, repository_account_name_map_errors := (*(repository_account_name_records_data_array.GetValues()))[0].GetMap()
	if repository_account_name_map_errors != nil {
		errors = append(errors, repository_account_name_map_errors...)
	} else if  common.IsNil(read_records_domain_name_response) {
		errors = append(errors, fmt.Errorf("repository_account_name_map is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	repository_account_name, repository_account_name_errors := repository_account_name_map.GetString("name")
	if repository_account_name_errors != nil {
		errors = append(errors, repository_account_name_errors...)
	} else if  common.IsNil(repository_account_name) {
		errors = append(errors, fmt.Errorf("repository_account_name is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_repository_name_select := []string{"name"}
	read_records_repository_name_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&read_records_repository_name_select))

	read_records_repository_name_where := map[string]interface{}{"repository_id":*repository_id}
	read_records_repository_name_where_map := json.NewMapOfValues(&read_records_repository_name_where)

	read_records_repository_name_request := map[string]interface{}{"[queue]":"ReadRecords_Repository", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
	read_records_repository_name_request_map := json.NewMapOfValues(&read_records_repository_name_request)
	read_records_repository_name_request_map.SetArray("[select_fields]", read_records_repository_name_array)
	read_records_repository_name_request_map.SetMap("[where_fields]", read_records_repository_name_where_map)

	read_records_repository_name_request_response, read_records_repository_name_request_response_errors := processor.SendMessageToQueue(read_records_repository_name_request_map)
	if read_records_repository_name_request_response_errors != nil {
		errors = append(errors, read_records_repository_name_request_response_errors...)
	} else if  common.IsNil(read_records_repository_name_request_response) {
		errors = append(errors, fmt.Errorf("read_records_repository_name_request_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	repository_name_records_data_array, repository_name_records_data_array_errors := read_records_repository_name_request_response.GetArray("data")
	if repository_name_records_data_array_errors != nil {
		errors = append(errors, repository_name_records_data_array_errors...)
	} else if  common.IsNil(repository_name_records_data_array) {
		errors = append(errors, fmt.Errorf("repository_name_records_data_array is nil"))
	} else if len(*(repository_name_records_data_array.GetValues())) != 1 {
		errors = append(errors, fmt.Errorf("repository_name_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	repository_name_map, repository_name_map_errors := (*(repository_name_records_data_array.GetValues()))[0].GetMap()
	if repository_name_map_errors != nil {
		errors = append(errors, repository_name_map_errors...)
	} else if  common.IsNil(repository_name_map) {
		errors = append(errors, fmt.Errorf("repository_name_map is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	repository_name, repository_name_errors := repository_name_map.GetString("name")
	if repository_name_errors != nil {
		errors = append(errors, repository_name_errors...)
	} else if  common.IsNil(repository_name) {
		errors = append(errors, fmt.Errorf("repository_name is nil"))
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
	first_build_step.SetStringValue("parameters", "{}")


	next_command := map[string]interface{}{"[queue]":*name_of_next_step,"[queue_mode]":"PushBack","[async]":false, "[trace_id]":processor.GenerateTraceId()}
	next_command_map := json.NewMapOfValues(&next_command)
	next_command_map.SetMap("data", first_build_step)
	
	_, message_errors := processor.SendMessageToQueue(next_command_map)
	if message_errors != nil {
		return message_errors
	}
	
	return nil
}

func commandRunStartBuildBranchInstanceFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunStartBuildBranchInstance
	return &funcValue
}