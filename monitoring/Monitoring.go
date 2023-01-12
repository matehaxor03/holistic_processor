package monitoring

import (
	common "github.com/matehaxor03/holistic_common/common"
	"strconv"
	"runtime"
	"fmt"
)

func GetCPULoad() (float64, []error) {
	var errors []error
	bashCommand := common.NewBashCommand()
	shell_output, bash_errors := bashCommand.ExecuteUnsafeCommand("ps -A -o %cpu | awk '{cpu_count+=$1} END {print cpu_count}'", nil, nil)
	
	if bash_errors != nil && len(bash_errors) > 0 {
		return 0.0, bash_errors
	}

	if len(*shell_output) != 1 {
		errors = append(errors, fmt.Errorf("cpu output contained more than one line"))
		return 0.0, errors
	}

	cpu_as_string := (*shell_output)[0]

	cpu_value, cpu_value_error := strconv.ParseFloat(cpu_as_string, 64)
	if cpu_value_error != nil {
		errors = append(errors, cpu_value_error)
	}

	if len(errors) > 0 {
		return 0.0, errors
	}

	return cpu_value, nil
}

func GetMemoryLoad() (float64, []error) {
	var errors []error
	bashCommand := common.NewBashCommand()
	shell_output, bash_errors := bashCommand.ExecuteUnsafeCommand("ps -A -o %mem | awk '{memory_count+=$1} END {print memory_count}'", nil, nil)
	
	if bash_errors != nil && len(bash_errors) > 0 {
		return 0.0, bash_errors
	}

	if len(*shell_output) != 1 {
		errors = append(errors, fmt.Errorf("memory output contained more than one line"))
		return 0.0, errors
	}

	memory_string := (*shell_output)[0]

	memory_value, memory_value_error := strconv.ParseFloat(memory_string, 64)
	if memory_value_error != nil {
		errors = append(errors, memory_value_error)
	}

	if len(errors) > 0 {
		return 0.0, errors
	}

	return memory_value, nil
}

func GetCPUVirtualCores() int {
	return runtime.NumCPU()
}

