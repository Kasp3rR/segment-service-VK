package service

import (
	"fmt"
	"regexp"
	"unicode/utf8"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

var codePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

func invalid(message string) error {
	return fmt.Errorf("%w: %s", domain.ErrInvalid, message)
}

func validName(name string) bool {
	length := utf8.RuneCountInString(name)
	return length >= 1 && length <= 200
}

func validDescription(description string) bool {
	return utf8.RuneCountInString(description) <= 4000
}
