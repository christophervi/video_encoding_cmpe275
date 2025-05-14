from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WorkerLoadInfo(_message.Message):
    __slots__ = ("address", "cpu_utilization", "active_tasks", "memory_usage_bytes", "worker_id")
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CPU_UTILIZATION_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_TASKS_FIELD_NUMBER: _ClassVar[int]
    MEMORY_USAGE_BYTES_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    address: str
    cpu_utilization: float
    active_tasks: int
    memory_usage_bytes: int
    worker_id: str
    def __init__(self, address: _Optional[str] = ..., cpu_utilization: _Optional[float] = ..., active_tasks: _Optional[int] = ..., memory_usage_bytes: _Optional[int] = ..., worker_id: _Optional[str] = ...) -> None: ...

class GetHealthyWorkersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetHealthyWorkersResponse(_message.Message):
    __slots__ = ("workers_with_load",)
    WORKERS_WITH_LOAD_FIELD_NUMBER: _ClassVar[int]
    workers_with_load: _containers.RepeatedCompositeFieldContainer[WorkerLoadInfo]
    def __init__(self, workers_with_load: _Optional[_Iterable[_Union[WorkerLoadInfo, _Mapping]]] = ...) -> None: ...
