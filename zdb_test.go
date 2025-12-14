package zdb

var (
	_ DB = zDB{}
	_ DB = zTX{}
)
