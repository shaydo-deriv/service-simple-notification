package main

func handleRPC(rpc string, args map[string]any, stash []string) (any, error) {
	ret := make([]map[string]any, 0)
	ret = append(ret, map[string]any{
		"id":      1,
		"payload": "SomeImportantContentHere",
	})
	ret = append(ret, map[string]any{
		"id":      2,
		"payload": "SomeOtherImportantContentHere",
	})
	return ret, nil
}
