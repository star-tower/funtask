rpc-gen:
	python -m grpc_tools.protoc -I funtask/schema/ \
	--python_betterproto_out=funtask/generated \
	funtask/schema/*.proto