def with_namespace(namespace: str, *values: str) -> str:
    return '#'.join([namespace, *values])
