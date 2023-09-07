# leveldblab

- usecase1: backup - merge leveldb
- usecase2: main - backup riêng biệt
- usecase3: normal leveldb

# Command
Test usecase1 với 10 thread đọc - 10 thread ghi trong vòng 300s:

go run main.go usecase1 --write=10 --read=10 --duration=300s

go run main.go usecase3 --write=10 --read=10 --duration=300s