filename?=tmp_$(shell date -Iseconds)

start:
	EnableBackup=true \
	EnableWriting=true \
	RootFolder="data" \
	go run main.go

check:
	EnableBackup=false \
	EnableWriting=false \
	RootFolder=${RootFolder} \
	go run main.go
