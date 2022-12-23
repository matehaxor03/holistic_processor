package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func commandRunStartBuildBranchInstance(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error

	request_keys := request.Keys()
	request_inner_map, request_inner_map_errors := request.GetMap(request_keys[0])
	if request_inner_map_errors != nil {
		errors = append(errors, request_inner_map_errors...)
	} else if common.IsNil(request_inner_map) {
		errors = append(errors, fmt.Errorf("request inner json is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 

	request_data, request_data_errors := request_inner_map.GetMap("data")
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

	callback_inner := json.Map{"[trace_id]":processor.GenerateTraceId(), "[order_by]":json.Array{json.Map{"order":"ascending"}}}
	callback_payload := json.Map{"ReadRecords_BuildStep":callback_inner}
	response, response_errors := processor.SendMessageToQueue(&callback_payload)
	if response_errors != nil {
		errors = append(errors, response_errors...)
	} else if common.IsNil(response) {
		errors = append(errors, fmt.Errorf("response is nil"))
	}

	if len(errors) > 0 {
		return errors
	} 

	keys := (*response).Keys()
	message_type := keys[0]
	payload_inner, payload_inner_errors := (*response).GetMap(message_type)
	if payload_inner_errors != nil {
		errors = append(errors, payload_inner_errors...)
	} else if common.IsNil(payload_inner) {
		errors = append(errors, fmt.Errorf("payload inner is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	if !payload_inner.HasKey("data") {
		errors = append(errors, fmt.Errorf("data not found"))
	} else if !payload_inner.IsArray("data") {
		errors = append(errors, fmt.Errorf("data is not an array"))
	} 

	if len(errors) > 0 {
		return errors
	}

	build_steps, build_steps_array := payload_inner.GetArray("data")
	if build_steps_array != nil {
		errors = append(errors, build_steps_array...)
	} else if common.IsNil(build_steps) {
		errors = append(errors, fmt.Errorf("build steps is nil"))
	} else if len(*build_steps) == 0 {
		errors = append(errors, fmt.Errorf("no build steps were found"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_branch_instance_steps := json.Array{}
	for _, build_step_interface := range *build_steps {
		if !common.IsMap(build_step_interface) {
			errors = append(errors, fmt.Errorf("build step is not a map"))
		}

		if len(errors) > 0 {
			return errors
		}

		var current_build_step json.Map
		type_of := common.GetType(build_step_interface)

		if type_of == "json.Map" {
			current_build_step = build_step_interface.(json.Map)
		} else if type_of == "*json.Map" {
			current_build_step = *(build_step_interface.(*json.Map))
		} else {
			errors = append(errors, fmt.Errorf("build step has invalid type"))
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

		build_branch_instance_step := json.Map{"build_branch_instance_id":*build_branch_instance_id, "build_step_id":*build_step_id, "order":*order}
		build_branch_instance_steps = append(build_branch_instance_steps, build_branch_instance_step)
	}
	

	create_instance_steps_request := json.Map{"CreateRecords_BuildBranchInstanceStep":json.Map{"[trace_id]":processor.GenerateTraceId(), "data":build_branch_instance_steps}}
	create_instance_steps_response, create_instance_steps_response_errors := processor.SendMessageToQueue(&create_instance_steps_request)
	if create_instance_steps_response_errors != nil {
		errors = append(errors, create_instance_steps_response_errors...)
	} else if common.IsNil(create_instance_steps_response) {
		errors = append(errors, fmt.Errorf("create_instance_steps_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}


	read_records_build_branch_instance_step_request := json.Map{"ReadRecords_BuildBranchInstanceStep":json.Map{"[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id", "order"},  "[where_fields]":json.Map{"build_branch_instance_id":*build_branch_instance_id}, "[limit]":1, "[order_by]":json.Array{json.Map{"order":"ascending"}}}}
	read_records_build_branch_instance_step_response, read_records_build_branch_instance_step_response_errors := processor.SendMessageToQueue(&read_records_build_branch_instance_step_request)
	if read_records_build_branch_instance_step_response_errors != nil {
		errors = append(errors, read_records_build_branch_instance_step_response_errors...)
	} else if common.IsNil(read_records_build_branch_instance_step_response) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_build_branch_instance_step_response_keys := read_records_build_branch_instance_step_response.Keys()
	read_records_build_branch_instance_step_response_key := read_records_build_branch_instance_step_response_keys[0]
	read_records_build_branch_instance_step_response_inner, read_records_build_branch_instance_step_response_inner_errors  := (*read_records_build_branch_instance_step_response).GetMap(read_records_build_branch_instance_step_response_key)
	if read_records_build_branch_instance_step_response_inner_errors != nil {
		errors = append(errors, read_records_build_branch_instance_step_response_inner_errors...)
	} else if common.IsNil(read_records_build_branch_instance_step_response_inner) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response_inner is nil"))
	}

	if len(errors) > 0 {
		return errors
	}
	
	first_build_step_array, first_build_step_array_errors := read_records_build_branch_instance_step_response_inner.GetArray("data")
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

	read_records_build_step_request := json.Map{"ReadRecords_BuildStep":json.Map{"[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"build_step_id":*desired_build_step_id}, "[select_fields]": json.Array{"name"}, "[limit]":1}}
	read_records_build_step_response, read_records_build_step_response_errors := processor.SendMessageToQueue(&read_records_build_step_request)
	if read_records_build_step_response_errors != nil {
		errors = append(errors, read_records_build_step_response_errors...)
	} else if common.IsNil(read_records_build_step_response) {
		errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_build_step_response_keys := read_records_build_step_response.Keys()
	read_records_build_step_response_key := read_records_build_step_response_keys[0]
	read_records_build_step_response_key_response_inner, read_records_build_step_response_key_response_inner_errors  := (*read_records_build_step_response).GetMap(read_records_build_step_response_key)
	if read_records_build_step_response_key_response_inner_errors != nil {
		errors = append(errors, read_records_build_step_response_key_response_inner_errors...)
	} else if common.IsNil(read_records_build_step_response_key_response_inner) {
		errors = append(errors, fmt.Errorf("read_records_build_step_response_key_response_inner is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	lookup_build_step_array, lookup_build_step_array_errors := read_records_build_step_response_key_response_inner.GetArray("data")
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

	
	read_records_build_branch_request := json.Map{"ReadRecords_BuildBranch":json.Map{"[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"build_branch_id":*build_branch_id}, "[select_fields]": json.Array{"build_id", "branch_id"}, "[limit]":1}}
	build_branch_records, build_branch_records_errors := processor.SendMessageToQueue(&read_records_build_branch_request)
	if build_branch_records_errors != nil {
		errors = append(errors, build_branch_records_errors...)
	} else if  common.IsNil(build_branch_records) {
		errors = append(errors, fmt.Errorf("build_branch_records is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_branch_records_map, build_branch_records_map_errors := build_branch_records.GetMap(build_branch_records.Keys()[0])
	if build_branch_records_map_errors != nil {
		errors = append(errors, build_branch_records_map_errors...)
	} else if  common.IsNil(build_branch_records_map) {
		errors = append(errors, fmt.Errorf("build_branch_records_map is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_branch_records_data_array, build_branch_records_data_array_errors := build_branch_records_map.GetArray("data")
	if build_branch_records_data_array_errors != nil {
		errors = append(errors, build_branch_records_data_array_errors...)
	} else if  common.IsNil(build_branch_records_data_array) {
		errors = append(errors, fmt.Errorf("build_branch_records_data_array is nil"))
	} else if len(*build_branch_records_data_array) != 1 {
		errors = append(errors, fmt.Errorf("build_branch_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	var build_branch json.Map
	build_branch_interface := (*build_branch_records_data_array)[0]
	type_of_build_branch_interface := common.GetType(build_branch_interface)

	if type_of_build_branch_interface == "json.Map" {
		build_branch = build_branch_interface.(json.Map)
	} else if type_of_build_branch_interface == "*json.Map" {
		build_branch = *(build_branch_interface.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("build_branch has invalid type"))
	}

	if len(errors) > 0 {
		return errors
	}

	branch_id, branch_id_errors := build_branch.GetUInt64("branch_id")
	if branch_id_errors != nil {
		errors = append(errors, branch_id_errors...)
	} else if  common.IsNil(build_branch_records_map) {
		errors = append(errors, fmt.Errorf("branch_id is nil"))
	}

	build_id, build_id_errors := build_branch.GetUInt64("build_id")
	if build_id_errors != nil {
		errors = append(errors, build_id_errors...)
	} else if  common.IsNil(build_branch_records_map) {
		errors = append(errors, fmt.Errorf("build_id is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	read_records_branch_request := json.Map{"ReadRecords_Branch":json.Map{"[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"branch_id":*branch_id}, "[select_fields]": json.Array{"name"}, "[limit]":1}}
	read_records_branch_response, read_records_branch_response_errors := processor.SendMessageToQueue(&read_records_branch_request)
	if read_records_branch_response_errors != nil {
		errors = append(errors, read_records_branch_response_errors...)
	} else if  common.IsNil(read_records_branch_response) {
		errors = append(errors, fmt.Errorf("read_records_branch_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	branch_records_map, branch_records_map_errors := read_records_branch_response.GetMap(read_records_branch_response.Keys()[0])
	if branch_records_map_errors != nil {
		errors = append(errors, branch_records_map_errors...)
	} else if  common.IsNil(branch_records_map) {
		errors = append(errors, fmt.Errorf("branch_records_map is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	branch_records_data_array, branch_records_data_array_errors := branch_records_map.GetArray("data")
	if branch_records_data_array_errors != nil {
		errors = append(errors, branch_records_data_array_errors...)
	} else if common.IsNil(branch_records_data_array) {
		errors = append(errors, fmt.Errorf("branch_records_data_array is nil"))
	} else if len(*branch_records_data_array) != 1 {
		errors = append(errors, fmt.Errorf("branch_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	var branch json.Map
	branch_interface := (*branch_records_data_array)[0]
	type_of_branch_interface := common.GetType(branch_interface)

	if type_of_branch_interface == "json.Map" {
		branch = branch_interface.(json.Map)
	} else if type_of_branch_interface == "*json.Map" {
		branch = *(branch_interface.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("branch has invalid type"))
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

	read_records_build_request := json.Map{"ReadRecords_Build":json.Map{"[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"build_id":*build_id}, "[select_fields]": json.Array{"domain_name_id", "repository_account_id", "repository_id"}, "[limit]":1}}
	read_records_build_response, read_records_build_response_errors := processor.SendMessageToQueue(&read_records_build_request)
	if read_records_build_response_errors != nil {
		errors = append(errors, read_records_build_response_errors...)
	} else if  common.IsNil(read_records_branch_response) {
		errors = append(errors, fmt.Errorf("read_records_build_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_records_map, build_records_map_errors := read_records_build_response.GetMap(read_records_build_response.Keys()[0])
	if build_records_map_errors != nil {
		errors = append(errors, build_records_map_errors...)
	} else if  common.IsNil(branch_records_map) {
		errors = append(errors, fmt.Errorf("build_records_map is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	build_records_data_array, build_records_data_array_errors := build_records_map.GetArray("data")
	if build_records_data_array_errors != nil {
		errors = append(errors, build_records_data_array_errors...)
	} else if  common.IsNil(build_records_data_array) {
		errors = append(errors, fmt.Errorf("build_records_data_array is nil"))
	} else if len(*build_records_data_array) != 1 {
		errors = append(errors, fmt.Errorf("build_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	var build json.Map
	build_interface := (*build_records_data_array)[0]
	type_of_build_interface := common.GetType(build_interface)

	if type_of_build_interface == "json.Map" {
		build = build_interface.(json.Map)
	} else if type_of_build_interface == "*json.Map" {
		build = *(build_interface.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("build has invalid type"))
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

	read_records_domain_name_request := json.Map{"ReadRecords_DomainName":json.Map{"[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"domain_name_id":*domain_name_id}, "[select_fields]": json.Array{"name"}, "[limit]":1}}
	read_records_domain_name_response, read_records_domain_name_response_errors := processor.SendMessageToQueue(&read_records_domain_name_request)
	if read_records_domain_name_response_errors != nil {
		errors = append(errors, read_records_domain_name_response_errors...)
	} else if  common.IsNil(read_records_domain_name_response) {
		errors = append(errors, fmt.Errorf("read_records_domain_name_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	domain_name_records_map, domain_name_records_map_errors := read_records_domain_name_response.GetMap(read_records_domain_name_response.Keys()[0])
	if domain_name_records_map_errors != nil {
		errors = append(errors, domain_name_records_map_errors...)
	} else if  common.IsNil(domain_name_records_map) {
		errors = append(errors, fmt.Errorf("domain_name_records_map is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	domain_name_records_data_array, domain_name_records_data_array_errors := domain_name_records_map.GetArray("data")
	if domain_name_records_data_array_errors != nil {
		errors = append(errors, domain_name_records_data_array_errors...)
	} else if  common.IsNil(domain_name_records_data_array) {
		errors = append(errors, fmt.Errorf("domain_name_records_data_array is nil"))
	} else if len(*domain_name_records_data_array) != 1 {
		errors = append(errors, fmt.Errorf("domain_name_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	var domain_name_map json.Map
	domain_name_interface := (*domain_name_records_data_array)[0]
	type_of_domain_name_interface := common.GetType(domain_name_interface)

	if type_of_domain_name_interface == "json.Map" {
		domain_name_map = domain_name_interface.(json.Map)
	} else if type_of_domain_name_interface == "*json.Map" {
		domain_name_map = *(domain_name_interface.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("domain_name has invalid type"))
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

	read_records_repository_account_name_request := json.Map{"ReadRecords_RepositoryAccount":json.Map{"[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"repository_account_id":*repository_account_id}, "[select_fields]": json.Array{"name"}, "[limit]":1}}
	read_records_repository_account_name_response, read_records_repository_account_name_response_errors := processor.SendMessageToQueue(&read_records_repository_account_name_request)
	if read_records_repository_account_name_response_errors != nil {
		errors = append(errors, read_records_repository_account_name_response_errors...)
	} else if  common.IsNil(read_records_domain_name_response) {
		errors = append(errors, fmt.Errorf("read_records_repository_account_name_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	repository_account_name_records_map, repository_account_name_records_map_errors := read_records_repository_account_name_response.GetMap(read_records_repository_account_name_response.Keys()[0])
	if repository_account_name_records_map_errors != nil {
		errors = append(errors, repository_account_name_records_map_errors...)
	} else if  common.IsNil(repository_account_name_records_map) {
		errors = append(errors, fmt.Errorf("repository_account_name_records_map is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	repository_account_name_records_data_array, repository_account_name_records_data_array_errors := repository_account_name_records_map.GetArray("data")
	if repository_account_name_records_data_array_errors != nil {
		errors = append(errors, repository_account_name_records_data_array_errors...)
	} else if  common.IsNil(repository_account_name_records_data_array) {
		errors = append(errors, fmt.Errorf("repository_account_name_records_data_array is nil"))
	} else if len(*repository_account_name_records_data_array) != 1 {
		errors = append(errors, fmt.Errorf("repository_account_name_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	var repository_account_name_map json.Map
	repository_account_name_interface := (*repository_account_name_records_data_array)[0]
	type_of_repository_account_name_interface := common.GetType(repository_account_name_interface)

	if type_of_repository_account_name_interface == "json.Map" {
		repository_account_name_map = repository_account_name_interface.(json.Map)
	} else if type_of_repository_account_name_interface == "*json.Map" {
		repository_account_name_map = *(repository_account_name_interface.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("repository_account_name has invalid type"))
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

	///

	read_records_repository_name_request := json.Map{"ReadRecords_Repository":json.Map{"[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"repository_id":*repository_id}, "[select_fields]": json.Array{"name"}, "[limit]":1}}
	read_records_repository_name_request_response, read_records_repository_name_request_response_errors := processor.SendMessageToQueue(&read_records_repository_name_request)
	if read_records_repository_name_request_response_errors != nil {
		errors = append(errors, read_records_repository_name_request_response_errors...)
	} else if  common.IsNil(read_records_repository_name_request_response) {
		errors = append(errors, fmt.Errorf("read_records_repository_name_request_response is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	repository_name_records_map, repository_name_records_map_errors := read_records_repository_name_request_response.GetMap(read_records_repository_name_request_response.Keys()[0])
	if repository_name_records_map_errors != nil {
		errors = append(errors, repository_name_records_map_errors...)
	} else if  common.IsNil(repository_name_records_map) {
		errors = append(errors, fmt.Errorf("repository_name_records_map is nil"))
	}

	if len(errors) > 0 {
		return errors
	}

	repository_name_records_data_array, repository_name_records_data_array_errors := repository_name_records_map.GetArray("data")
	if repository_name_records_data_array_errors != nil {
		errors = append(errors, repository_name_records_data_array_errors...)
	} else if  common.IsNil(repository_name_records_data_array) {
		errors = append(errors, fmt.Errorf("repository_name_records_data_array is nil"))
	} else if len(*repository_name_records_data_array) != 1 {
		errors = append(errors, fmt.Errorf("repository_name_records_data_array did not have one result"))
	}

	if len(errors) > 0 {
		return errors
	}

	var repository_name_map json.Map
	repository_name_interface := (*repository_name_records_data_array)[0]
	type_of_repository_name_interface := common.GetType(repository_name_interface)

	if type_of_repository_name_interface == "json.Map" {
		repository_name_map = repository_name_interface.(json.Map)
	} else if type_of_repository_name_interface == "*json.Map" {
		repository_name_map = *(repository_name_interface.(*json.Map))
	} else {
		errors = append(errors, fmt.Errorf("repository_name has invalid type"))
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
	
	next_command := json.Map{*name_of_next_step:json.Map{"data":first_build_step,"[queue_mode]":"PushBack","[async]":false, "[trace_id]":processor.GenerateTraceId()}}
	_, message_errors := processor.SendMessageToQueue(&next_command)
	if message_errors != nil {
		return message_errors
	}
	
	return nil
}

func commandRunStartBuildBranchInstanceFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunStartBuildBranchInstance
	return &funcValue
}