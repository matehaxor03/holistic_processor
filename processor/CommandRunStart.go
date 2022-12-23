package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	//common "github.com/matehaxor03/holistic_common/common"
	//"fmt"
)

func commandRunStart(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, errors := validateRunCommandHeaders(request)
	if errors != nil {
		return errors
	}

	// do something

	trigger_next_run_command_errors := triggerNextRunCommand(processor, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunStartFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunStart
	return &funcValue
}