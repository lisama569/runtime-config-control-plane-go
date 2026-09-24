package config

import "encoding/json"

// ParseProperty validates one property from type + JSON-encoded value.
func ParseProperty(key, typ string, value json.RawMessage) (Property, error) {
	return parseProperty(key, json.RawMessage(mustMarshal(RawProperty{Type: typ, Value: value})))
}

// ParseFeatureDef validates a feature from its raw form.
func ParseFeatureDef(name string, rf RawFeature) (Feature, error) {
	return parseFeature(name, json.RawMessage(mustMarshal(rf)))
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}
