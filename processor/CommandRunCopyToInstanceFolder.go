package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
    "path/filepath"
	"fmt"
)

func commandRunCopyToInstanceFolder(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, errors := validateRunCommandHeaders(request)
	if errors != nil {
		return errors
	} else {
		var new_errors []error
		errors = new_errors
	}

	std_callback := func(message string) {
		fmt.Println(message)
	}

	stderr_callback := func(message error) {
		fmt.Println(message)
	}
	//depending if doing by branch or tag


	// todo: validate directory names
	directory_parts := common.GetDataDirectory()
	directory_parts = append(directory_parts, "src")
	directory_parts = append(directory_parts, *domain_name)
	directory_parts = append(directory_parts, *repository_account_name)
	directory_parts = append(directory_parts, *repository_name)
	directory_parts = append(directory_parts, "branches")
	directory_parts = append(directory_parts, *branch_name)
	directory_parts = append(directory_parts, *repository_name)
	full_path_of_directory := filepath.Join(directory_parts...)

	instance_folder_parts := common.GetDataDirectory()
	instance_folder_parts = append(instance_folder_parts, "src")
	instance_folder_parts = append(instance_folder_parts, *domain_name)
	instance_folder_parts = append(instance_folder_parts, *repository_account_name)
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	instance_folder_parts = append(instance_folder_parts, "branch_instances")
	instance_folder_parts = append(instance_folder_parts, fmt.Sprintf("%d", *build_branch_instance_id))
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	full_path_of_instance_directory := filepath.Join(instance_folder_parts...)



	bashCommand := common.NewBashCommand()
	command := fmt.Sprintf("cp -r %s %s", "/" + full_path_of_directory,  "/" + full_path_of_instance_directory)
	_, bash_command_errors := bashCommand.ExecuteUnsafeCommand(command, &std_callback,  &stderr_callback)
	if bash_command_errors != nil {
		errors = append(errors, bash_command_errors...)
	} 

	if len(errors) > 0 {
		return errors
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunCopyToInstanceFolderFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunCopyToInstanceFolder
	return &funcValue
}