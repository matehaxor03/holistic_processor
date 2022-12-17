package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	//common "github.com/matehaxor03/holistic_common/common"
	"fmt"
	//"strings"
)

func commandRunBuildBranchInstance(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	var errors []error

	fmt.Println("!!!! running the build !!!!")

	if len(errors) > 0 {
		return errors
	} 

	return nil
}