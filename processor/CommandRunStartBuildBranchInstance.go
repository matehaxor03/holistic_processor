package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"fmt"
	"strings"
)

func commandRunStartBuildBranchInstance(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error

	fmt.Println("!!!! running the build !!!!")
	callback_inner := json.Map{"[trace_id]":processor.GenerateTraceId()}
	callback_payload := json.Map{"ReadRecords_BuildStep":callback_inner}
	response, response_errors := processor.SendMessageToQueue(&callback_payload)
	if response_errors != nil {
		errors = append(errors, response_errors...)
	} else if common.IsNil(response) {
		errors = append(errors, fmt.Errorf("response is nil"))
	} else {
		var builder strings.Builder
		response_json_string_errors := response.ToJSONString(&builder)
		if response_json_string_errors != nil {
			errors = append(errors, response_json_string_errors...)
		} else {
			fmt.Println(builder.String())
		}
	}

	if len(errors) > 0 {
		fmt.Println(errors)
		return errors
	} 

	return nil
}