package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	"fmt"
)

func commandRunEnd(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors := validateRunCommandHeaders(processor, request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} else if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	fmt.Println("todo: run end")

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunEndFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunEnd
	return &funcValue
}