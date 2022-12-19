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

	callback_inner := json.Map{"[trace_id]":processor.GenerateTraceId(), "[order_by]":json.Map{"order":"ascending"}}
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

		build_branch_instance_step := json.Map{"build_branch_instance_id":*build_branch_instance_id, "build_step_id":build_step_id}
		build_branch_instance_steps = append(build_branch_instance_steps, build_branch_instance_step)
	}
	
 

	

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