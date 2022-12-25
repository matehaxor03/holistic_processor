package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	"fmt"
)

func getStdoutCallbackFunctionBranch(processor *Processor, build_branch_instance_step_id uint64) (*func(message string)) {
	this_build_branch_instance_step_id := build_branch_instance_step_id
	this_processor := processor
	
	function := func(message string) {
		callback_payload := json.Map{"CreateRecord_BuildBranchInstanceStepLog":json.Map{"data":json.Map{"build_branch_instance_step_id":this_build_branch_instance_step_id,"log":message,"stdout":true},"[queue_mode]":"PushBack","[async]":true, "[trace_id]":this_processor.GenerateTraceId()}}
		processor.SendMessageToQueueFireAndForget(&callback_payload)
	}
	return &function
}

func getStderrCallbackFunctionBranch(processor *Processor, build_branch_instance_step_id uint64) (*func(message error)) {
	this_build_branch_instance_step_id := build_branch_instance_step_id
	this_processor := processor
	
	function := func(message error) {
		callback_payload := json.Map{"CreateRecord_BuildBranchInstanceStepLog":json.Map{"data":json.Map{"build_branch_instance_step_id":this_build_branch_instance_step_id,"log":fmt.Sprintf("%s",message),"stdout":false},"[queue_mode]":"PushBack","[async]":true, "[trace_id]":this_processor.GenerateTraceId()}}
		processor.SendMessageToQueueFireAndForget(&callback_payload)
	}
	return &function
}