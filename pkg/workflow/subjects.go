package workflow

import (
	"fmt"
	"strings"
)

// DefaultSubjectPrefix is the default prefix for workflow NATS subjects.
const DefaultSubjectPrefix = "workflow"

func subjectPrefix(prefix string) string {
	prefix = strings.Trim(prefix, ".")
	if prefix == "" {
		return DefaultSubjectPrefix
	}
	return prefix
}

// SubmitSubject returns the subject for workflow submission.
func SubmitSubject(prefix string) string {
	return subjectPrefix(prefix) + ".submit"
}

// ResultSubject returns the subject for a specific task result.
func ResultSubject(prefix, taskID string) string {
	return fmt.Sprintf("%s.result.%s", subjectPrefix(prefix), taskID)
}

// ResultFilterSubject returns the subject filter for all task results.
func ResultFilterSubject(prefix string) string {
	return subjectPrefix(prefix) + ".result.>"
}

// CancelRequestSubject returns the subject for workflow cancellation requests.
func CancelRequestSubject(prefix string) string {
	return subjectPrefix(prefix) + ".cancel"
}

// CancelSubject returns the subject for a specific workflow cancellation.
func CancelSubject(prefix, workflowID string) string {
	return fmt.Sprintf("%s.cancel.%s", prefix, workflowID)
}
