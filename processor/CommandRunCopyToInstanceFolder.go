package processor

import (
	"fmt"
	"path/filepath"

	common "github.com/matehaxor03/holistic_common/common"
	json "github.com/matehaxor03/holistic_json/json"
)

func commandRunCopyToInstanceFolder(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
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
	destination_host_user, destination_host_user_errors := processor.CalculateDesintationHostUserName(*branch_instance_id)

	if destination_host_user_errors != nil {
		errors = append(errors, destination_host_user_errors...)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	// todo: validate directory names
	var directory_parts []string
	directory_parts = append(directory_parts, "src")
	directory_parts = append(directory_parts, *domain_name)
	directory_parts = append(directory_parts, *repository_account_name)
	directory_parts = append(directory_parts, *repository_name)
	directory_parts = append(directory_parts, "branches")
	directory_parts = append(directory_parts, *branch_name)
	directory_parts = append(directory_parts, *repository_name)
	full_path_of_directory := "~/" + filepath.Join(directory_parts...)

	var instance_folder_parts []string
	//instance_folder_parts = append(instance_folder_parts, "src")
	//instance_folder_parts = append(instance_folder_parts, *domain_name)
	//instance_folder_parts = append(instance_folder_parts, *repository_account_name)
	//instance_folder_parts = append(instance_folder_parts, *repository_name)
	instance_folder_parts = append(instance_folder_parts, "branch_instances")
	instance_folder_parts = append(instance_folder_parts, fmt.Sprintf("%d", *branch_instance_id))
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	full_path_of_instance_directory := "./" + filepath.Join(instance_folder_parts...)

	if destination_host_user_errors != nil {
		errors = append(errors, destination_host_user_errors...)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	bashCommand := common.NewBashCommand()
	copy_command := fmt.Sprintf("cd ~ && scp -o StrictHostKeyChecking=accept-new -i %s -r %s %s:%s", "~/.ssh/"+destination_host_user.GetFullyQualifiedUsername(), full_path_of_directory, destination_host_user.GetFullyQualifiedUsername(), full_path_of_instance_directory)
	fmt.Println(copy_command)
	bash_command_errors := bashCommand.ExecuteUnsafeCommandSimple(copy_command)
	if bash_command_errors != nil {
		errors = append(errors, bash_command_errors...)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunCopyToInstanceFolderFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunCopyToInstanceFolder
	return &funcValue
}
