package httpapi

import "encoding/json"

type CreateSegmentRequest struct {
	Code        string `json:"code"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type UpdateSegmentRequest struct {
	Name        optionalString `json:"name"`
	Description optionalString `json:"description"`
}

type CreateUserRequest struct {
	ID int64 `json:"user_id"`
}

type CreateDistributionRequest struct {
	Percent json.RawMessage `json:"percent"`
}
