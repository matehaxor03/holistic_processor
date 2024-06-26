package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"os"
    "path/filepath"
	"fmt"
	"strings"
)

func commandRunCloneBranchOrTagFolder(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
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

	host_user := processor.GetHostUser()
	home_directory, home_directory_errors := host_user.GetHomeDirectoryAbsoluteDirectory()
	if home_directory_errors != nil {
		errors = append(errors, home_directory_errors...)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	// todo: validate directory names
	directory_parts := home_directory.GetPath()
	directory_parts = append(directory_parts, "src")
	directory_parts = append(directory_parts, *domain_name)
	directory_parts = append(directory_parts, *repository_account_name)
	directory_parts = append(directory_parts, *repository_name)
	directory_parts = append(directory_parts, "branches")
	directory_parts = append(directory_parts, *branch_name)
	directory_parts = append(directory_parts, *repository_name)

	full_path_of_directory := filepath.Join(directory_parts...)

	if _, stat_error := os.Stat("/" + full_path_of_directory); os.IsNotExist(stat_error) {
		bashCommand := common.NewBashCommand()
		command := fmt.Sprintf("git clone --branch %s git@%s:%s/%s.git %s", *branch_name, *domain_name, *repository_account_name, *repository_name, "/" + full_path_of_directory)
		_, bash_command_errors := bashCommand.ExecuteUnsafeCommandUsingFilesWithoutInputFile(command)
		if bash_command_errors != nil && len(bash_command_errors) > 0 {
			if !strings.Contains(fmt.Sprintf("%s", bash_command_errors[0]), "Cloning into") {
				errors = append(errors, bash_command_errors...)
			}
		} 
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunCloneBranchOrTagFolderFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunCloneBranchOrTagFolder
	return &funcValue
}