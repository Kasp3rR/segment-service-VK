package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func decode(w http.ResponseWriter, r *http.Request, value any) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(value); err == nil {
		var extra any
		if decodeErr := decoder.Decode(&extra); decodeErr != io.EOF {
			if decodeErr != nil {
				err = decodeErr
			} else {
				err = fmt.Errorf("exactly one JSON object is required")
			}
		}
	}
	if err != nil {
		var size *http.MaxBytesError
		if errors.As(err, &size) {
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "body must not exceed 64 KiB")
		} else {
			writeError(w, http.StatusBadRequest, "invalid_json", "invalid JSON object or unsupported field")
		}
		return false
	}
	return true
}

func pathID(c *gin.Context, key string) (int64, error) {
	id, err := strconv.ParseInt(c.Param(key), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%w: %s must be a positive integer", domain.ErrInvalid, key)
	}
	return id, nil
}

type optionalString struct {
	value *string
}

func (field *optionalString) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return errors.New("null is not supported")
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	field.value = &value
	return nil
}
