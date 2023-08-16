package main

import (
	"context"
	"fmt"

	mokafka "github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka"
)

func main() {
	// Sample configuration
	configs := map[string]interface{}{
		"type":              "kafka",
		"topic":             "user",
		"partition":         "1",
		"value":             "protobuf_sr",
		"bootstrap.servers": "127.0.0.1:62610",
		"sasl.username":     "",
		"sasl.password":     "",
		"sasl.mechanisms":   "",
		"security.protocol": "",
	}

	// Sample attributes and types
	//attrs := []string{"Id", "Name", "Email", "Phone"}
	//types := []types.Type{
	//	{Oid: types.T_varchar},
	//	{Oid: types.T_varchar},
	//	{Oid: types.T_varchar},
	//	{Oid: types.T_varchar},
	//}

	// Validate and prepare the configuration
	err := mokafka.ValidateConfig(context.Background(), configs)
	if err != nil {
		fmt.Printf("Error in ValidateAndPrepareConfig: %s\n", err)
		return
	}

	//// Retrieve data using the configuration
	//ctx := context.Background()
	//mp := mpool.New(8192)
	//b, err := RetrieveData(ctx, configs, attrs, types, 0, 10, mp)
	//if err != nil {
	//	fmt.Printf("Error in RetrieveData: %s\n", err)
	//	return
	//}

	//// Print the retrieved batch (for demonstration purposes)
	//fmt.Println(b)
}
