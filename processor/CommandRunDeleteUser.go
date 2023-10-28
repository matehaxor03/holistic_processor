package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"strconv"
)

func commandRunDeleteUser(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors := validateRunCommandHeaders(processor, request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} else if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	std_callback := getStdoutCallbackFunctionBranch(processor, *command_name, *branch_instance_id, *branch_instance_step_id, *branch_id, "")
	stderr_callback := getStderrCallbackFunctionBranch(processor, *command_name, *branch_instance_id, *branch_instance_step_id, *branch_id, "")

	time_format := "20060102"
	branch_instance_id_string := strconv.FormatUint(*branch_instance_id, 10)
	if len(branch_instance_id_string) > 10 {
		branch_instance_id_string = branch_instance_id_string[len(branch_instance_id_string)-10:]
	}
	
	shell_command := "dscl . -delete /Users/holisticxyz_b" + created_date.Format(time_format) + branch_instance_id_string
	(*std_callback)(shell_command)
	bashCommand := common.NewBashCommand()

	stdout_lines, bash_errors := bashCommand.ExecuteUnsafeCommandUsingFilesWithoutInputFile(shell_command)

	if bash_errors != nil {
		errors = append(errors, bash_errors...)
	}

	if stdout_lines != nil {
		for _, stdout_line := range stdout_lines {
			(*std_callback)(stdout_line)
		}
	}

	if errors != nil {
		for _, e := range errors {
			(*stderr_callback)(e)
		}
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunDeleteUserFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunDeleteUser
	return &funcValue
}