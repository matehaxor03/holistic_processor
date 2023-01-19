package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
    "path/filepath"
	"os"
	"fmt"
)

func commandRunDeleteInstanceFolder(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors := validateRunCommandHeaders(processor, request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} else if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	// todo: validate directory names do things depending if branch or tag
	directory_parts := common.GetDataDirectory()
	directory_parts = append(directory_parts, "src")
	directory_parts = append(directory_parts, *domain_name)
	directory_parts = append(directory_parts, *repository_account_name)
	directory_parts = append(directory_parts, *repository_name)
	directory_parts = append(directory_parts, "branch_instances")
	directory_parts = append(directory_parts, fmt.Sprintf("%d", *build_branch_instance_id))
	full_path_of_directory := "/" + filepath.Join(directory_parts...)

	if _, stat_error := os.Stat(full_path_of_directory); !os.IsNotExist(stat_error) {
		bashCommand := common.NewBashCommand()
		command := fmt.Sprintf("rm -fr %s", full_path_of_directory)
		_, bash_command_errors := bashCommand.ExecuteUnsafeCommandUsingFilesWithoutInputFile(command)
		if bash_command_errors != nil {
			errors = append(errors, bash_command_errors...)
		} 
	}

	

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors,  request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunDeleteInstanceFolderFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunDeleteInstanceFolder
	return &funcValue
}