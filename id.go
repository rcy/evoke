package evoke

import "github.com/google/uuid"

// Make a uuid from a string
func UUID(str string) uuid.UUID {
	return uuid.NewSHA1(uuid.Nil, []byte(str))
}
