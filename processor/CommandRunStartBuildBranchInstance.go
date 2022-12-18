package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	//common "github.com/matehaxor03/holistic_common/common"
	"fmt"
	//"strings"
)

func commandRunStartBuildBranchInstance(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error

	fmt.Println("!!!! running the build !!!!")
	//callback_inner := json.Map{"data":new_record_fields,"[queue_mode]":"PushBack","[async]":false, "[trace_id]":processor.GenerateTraceId()}
	//callback_payload := json.Map{"ReadRecords_BuildStep":callback_inner}
	//reaponse, response_errors := processor.CallQueue(&callback_payload)


	if len(errors) > 0 {
		return errors
	} 

	return nil
}