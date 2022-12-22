package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
)

func commandRunStartBuildBranchInstance(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error

	fmt.Println("!!!! running the build !!!!")
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


	read_records_build_branch_instance_step_request := json.Map{"ReadRecords_BuildBranchInstanceStep":json.Map{"[trace_id]":processor.GenerateTraceId(), "[select_fields]": json.Array{"build_branch_instance_step_id", "build_branch_instance_id", "build_step_id"}, "[limit]":1, "[order_by]":json.Array{json.Map{"order":"ascending"}}}}
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

	fmt.Println(first_build_step)

	// get the step name
	
	//todo run first step
 

	

		/*var builder strings.Builder
		response_json_string_errors := response.ToJSONString(&builder)
		if response_json_string_errors != nil {
			errors = append(errors, response_json_string_errors...)
		} else {
			fmt.Println(builder.String())
		}*/
	

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	} 

	return nil
}