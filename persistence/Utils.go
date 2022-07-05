package persistence

import (
	"encoding/json"
	"strconv"
	"strings"
)

// GenerateParameters generates a list of value parameters to use in SQL statements like: "$1,$2,$3"
//	Parameters:
//		- values an array with column values or a key-value map
//	Returns: a generated list of value parameters
func GenerateParameters[T any](values any) string {

	result := strings.Builder{}
	// String arrays
	if val, ok := values.([]T); ok {
		for index := 1; index <= len(val); index++ {
			if result.String() != "" {
				result.WriteString(",")
			}
			result.WriteString("$")
			result.WriteString(strconv.FormatInt((int64)(index), 10))
		}

		return result.String()
	}

	items := ConvertToMap(values)
	if items == nil {
		return ""
	}

	for index := 1; index <= len(items); index++ {
		if result.String() != "" {
			result.WriteString(",")
		}
		result.WriteString("$")
		result.WriteString(strconv.FormatInt((int64)(index), 10))
	}

	return result.String()
}

func ConvertToMap(values any) map[string]any {
	mRes, mErr := json.Marshal(values)
	if mErr != nil {
		return nil
	}
	items := make(map[string]any, 0)
	mErr = json.Unmarshal(mRes, &items)
	if mErr != nil {
		return nil
	}
	return items
}
