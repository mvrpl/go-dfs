protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative fsServer.proto

Move-item -Path .\fsServer_grpc.pb.go -destination .\src\fileSystemServer\fileSystemServer.go -force
Move-item -Path .\fsServer.pb.go -destination .\src\fileSystemServer\fsServer.go -force