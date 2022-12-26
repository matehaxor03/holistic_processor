package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"strings"
	"fmt"
	"time"
)

func getStdoutCallbackFunctionBranch(processor *Processor, command_name string, build_branch_id uint64, build_branch_instance_step_id uint64) (*func(message string)) {
	this_processor := processor
	this_command_name := command_name
	this_build_branch_id := build_branch_id
	this_build_branch_instance_step_id := build_branch_instance_step_id
	this_count := 0
	
	function := func(message string) {
		time.Sleep(1 * time.Nanosecond) 
		callback_payload := json.Map{"[queue]":"CreateRecord_BuildBranchInstanceStepLog", "data":json.Map{"build_branch_instance_step_id":this_build_branch_instance_step_id,"log":message,"stdout":true},"[queue_mode]":"PushBack","[async]":true, "[trace_id]":this_processor.GenerateTraceId()}
		this_processor.SendMessageToQueueFireAndForget(&callback_payload)

		if this_command_name != "Run_IntegrationTests" {
			return
		}
		
		message = strings.ReplaceAll(message, "\\\"", "\"")
		json_message, json_message_errors := json.ParseJSON(message)
		if json_message_errors != nil {
			fmt.Println(json_message_errors)
			return
		}

		if json_message == nil {
			fmt.Println("json_message is nil")
			return
		}

		test_suite_value, test_suite_value_errors := json_message.GetString("Package")
		if test_suite_value_errors != nil {
			fmt.Println(test_suite_value_errors)
			return
		}

		if test_suite_value == nil {
			fmt.Println("test_suite_value is nil")
			return
		}

		test_value, test_value_errors := json_message.GetString("Test")

		if test_value_errors != nil {
			fmt.Println(test_value_errors)
			return
		}

		if test_value == nil {
			fmt.Println("test_value is nil " + message)
			return
		}

		test_result_value, test_result_value_errors := json_message.GetString("Action")

		if test_result_value_errors != nil {
			fmt.Println(test_result_value_errors)
			return
		}

		if test_result_value == nil {
			fmt.Println("test_result_value is nil")
			return
		}

		if *test_result_value == "pass" {
			*test_result_value = "Passed"
		} else if *test_result_value == "fail" {
			*test_result_value = "Failed"
		} else {
			return
		}

		this_count++
		fmt.Println(this_count)

		time.Sleep(1 * time.Nanosecond) 
		select_test_suite_payload := json.Map{"[queue]":"ReadRecords_TestSuiteBuildBranch", "[where_fields]":json.Map{"build_branch_id":this_build_branch_id,"name":*test_suite_value}, "[limit]":1,"[queue_mode]":"PushBack","[async]":false,"[trace_id]":this_processor.GenerateTraceId()}
		test_suite_response, test_suite_response_errors := this_processor.SendMessageToQueue(&select_test_suite_payload)
		if test_suite_response_errors != nil {
			fmt.Println(test_suite_response_errors)
			return
		}

		if test_suite_response == nil {
			fmt.Println("test_suite_response is nil")
			return
		}

		test_suite_response_array, test_suite_response_array_errors := test_suite_response.GetArray("data")
		if test_suite_response_array_errors != nil {
			fmt.Println(test_suite_response_array_errors)
			return
		}

		if test_suite_response_array == nil {
			fmt.Println("test_suite_response_array is nil")
			return
		}

		var test_suite_map json.Map
		if len(*test_suite_response_array) == 0 {
			time.Sleep(1 * time.Nanosecond) 
			create_test_suite_payload := json.Map{"[queue]":"CreateRecord_TestSuiteBuildBranch", "data":json.Map{"build_branch_id":this_build_branch_id,"name":*test_suite_value},"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
			create_suite_response, create_suite_response_errors := this_processor.SendMessageToQueue(&create_test_suite_payload)
			if create_suite_response_errors != nil {
				fmt.Println(create_suite_response_errors)
				return
			}

			if create_suite_response == nil {
				fmt.Println("create_suite_response is nil")
				return
			}

			created_test_suite_map, created_test_suite_map_errors := create_suite_response.GetMap("data")
			if created_test_suite_map_errors != nil {
				fmt.Println(created_test_suite_map_errors)
				return 
			}

			if created_test_suite_map == nil {
				fmt.Println("created_test_suite_map is nil")
				return
			}

			test_suite_map = *created_test_suite_map
		} else if len(*test_suite_response_array) == 1 {
			
			test_suite_interface := (*test_suite_response_array)[0]
			type_of_test_suite_interface := common.GetType(test_suite_interface)

			if type_of_test_suite_interface == "json.Map" {
				test_suite_map = test_suite_interface.(json.Map)
			} else if type_of_test_suite_interface == "*json.Map" {
				test_suite_map = *(test_suite_interface.(*json.Map))
			} else {
				fmt.Println("test_suite_map is not a map")
				return
			}

		} else {
			fmt.Println("test_suite_response_array did not return 1 or 0 records")
			return
		}

		test_suite_build_branch_id, test_suite_build_branch_id_errors := test_suite_map.GetUInt64("test_suite_build_branch_id")
		if test_suite_build_branch_id_errors != nil {
			fmt.Println(test_suite_build_branch_id_errors)
			return
		}

		if test_suite_build_branch_id == nil {
			fmt.Println("test_suite_build_branch_id is nil")
			return
		}

		time.Sleep(1 * time.Nanosecond) 
		select_test_payload := json.Map{"[queue]":"ReadRecords_TestBuildBranch", "[where_fields]":json.Map{"test_suite_build_branch_id":*test_suite_build_branch_id,"name":*test_value}, "[limit]":1,"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
		test_response, test_response_errors := this_processor.SendMessageToQueue(&select_test_payload)
		if test_response_errors != nil {
			fmt.Println(test_response_errors)
			return
		}

		if test_response == nil {
			fmt.Println("test_response is nil")
			return
		}

		test_response_array, test_response_array_errors := test_response.GetArray("data")
		if test_response_array_errors != nil {
			fmt.Println(test_response_array_errors)
			return
		}

		if test_response_array == nil {
			fmt.Println("test_response_array is nil")
			return
		}

		var test_map json.Map
		if len(*test_response_array) == 0 {
			time.Sleep(1 * time.Nanosecond) 
			create_test_payload := json.Map{"[queue]":"CreateRecord_TestBuildBranch", "data":json.Map{"test_suite_build_branch_id":test_suite_build_branch_id,"name":*test_value},"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
			create_test_response, create_test_response_errors := this_processor.SendMessageToQueue(&create_test_payload)
			if create_test_response_errors != nil {
				fmt.Println(create_test_response_errors)
				return
			}

			if create_test_response == nil {
				fmt.Println("create_test_response is nil")
				return
			}

			created_test_map, created_test_map_errors := create_test_response.GetMap("data")
			if created_test_map_errors != nil {
				fmt.Println(created_test_map_errors)
				return 
			}

			if created_test_map == nil {
				fmt.Println("created_test_map is nil")
				return
			}

			test_map = *created_test_map
		} else if len(*test_response_array) == 1 {
			
			test_interface := (*test_response_array)[0]
			type_of_test_interface := common.GetType(test_interface)

			if type_of_test_interface == "json.Map" {
				test_map = test_interface.(json.Map)
			} else if type_of_test_interface == "*json.Map" {
				test_map = *(test_interface.(*json.Map))
			} else {
				fmt.Println("test_map is not a map")
				return
			}

		} else {
			fmt.Println("test_response_array returned more than 1 record")
			return
		}

	test_build_branch_id, test_build_branch_id_errors := test_map.GetUInt64("test_build_branch_id")
	if test_build_branch_id_errors != nil {
		fmt.Println(test_build_branch_id_errors)
		return
	}

	if test_build_branch_id == nil {
		fmt.Println("test_build_branch_id is nil")
		return
	}

	time.Sleep(1 * time.Nanosecond) 
	select_test_result_payload := json.Map{"[queue]":"ReadRecords_TestResult", "[where_fields]":json.Map{"name":*test_result_value}, "[limit]":1,"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
	test_result_response, test_result_response_errors := this_processor.SendMessageToQueue(&select_test_result_payload)
	if test_result_response_errors != nil {
		fmt.Println(test_result_response_errors)
		return
	}

	if test_result_response == nil {
		fmt.Println("test_result_response is nil")
		return
	}

	test_result_response_array, test_result_response_array_errors := test_result_response.GetArray("data")
	if test_result_response_array_errors != nil {
		fmt.Println(test_result_response_array_errors)
		return
	}

	if test_result_response_array == nil {
		fmt.Println("test_result_response_array is nil")
		return
	}

	var test_result_map json.Map
	if len(*test_result_response_array) == 1 {
		test_result_interface := (*test_result_response_array)[0]
		type_of_test_result_interface := common.GetType(test_result_interface)

		if type_of_test_result_interface == "json.Map" {
			test_result_map = test_result_interface.(json.Map)
		} else if type_of_test_result_interface == "*json.Map" {
			test_result_map = *(test_result_interface.(*json.Map))
		} else {
			fmt.Println("test_result_map is not a map")
			return
		}
	} else {
		fmt.Println("test_result_response_array did not have 1 record")
		return
	}

	test_result_id, test_result_id_errors := test_result_map.GetUInt64("test_result_id")
	if test_result_id_errors != nil {
		fmt.Println(test_result_id_errors)
		return 
	}

	if test_result_id == nil {
		fmt.Println("test_result_id is nil")
		return
	}

	time.Sleep(1 * time.Nanosecond) 
	create_test_log_payload := json.Map{"[queue]":"CreateRecord_BuildBranchInstanceStepTestResult", "data":json.Map{"build_branch_instance_step_id":build_branch_instance_step_id,"test_build_branch_id":*test_build_branch_id, "test_result_id":*test_result_id},"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
	create_test_log_response, create_test_log_response_errors := this_processor.SendMessageToQueue(&create_test_log_payload)
	if create_test_log_response_errors != nil {
		fmt.Println(create_test_log_response_errors)
		return
	}

	if create_test_log_response == nil {
		fmt.Println("create_test_log_response is nil")
		return
	}
	
	}	
	return &function
}

func getStderrCallbackFunctionBranch(processor *Processor, command_name string, build_branch_id uint64, build_branch_instance_step_id uint64) (*func(message error)) {
	this_processor := processor
	//this_command_name := command_name
	//this_build_branch_id := build_branch_id
	this_build_branch_instance_step_id := build_branch_instance_step_id
	
	
	function := func(message error) {
		callback_payload := json.Map{"[queue]":"CreateRecord_BuildBranchInstanceStepLog", "data":json.Map{"build_branch_instance_step_id":this_build_branch_instance_step_id,"log":fmt.Sprintf("%s",message),"stdout":false},"[queue_mode]":"PushBack","[async]":true, "[trace_id]":this_processor.GenerateTraceId()}
		this_processor.SendMessageToQueueFireAndForget(&callback_payload)
	}
	return &function
}