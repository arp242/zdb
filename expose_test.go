package zdb

import (
	"bytes"
	"os"
	"testing"
)

// Expose some things for tests.

var (
	E_prepareImpl = prepareImpl
)

type (
	E_dbImpl   = dbImpl
	E_zTX      = zTX
	E_zDB      = zDB
	E_logDB    = logDB
	E_metricDB = metricDB
)

func BufStderr(t *testing.T) *bytes.Buffer {
	buf := new(bytes.Buffer)
	stderr = buf
	t.Cleanup(func() { stderr = os.Stderr })
	return buf
}
