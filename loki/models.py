from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, Any
from pydantic import BaseModel, Field


class LokiBaseResponse(BaseModel):
    status: Optional[str] = Field(regex='success')
    data: Union[Any, List[str]]

    def __len__(self):
        return len(self.data)


class LokiBaseData(BaseModel):
    resultType: str
    result: List[Any]

    def __len__(self):
        return len(self.result)


class LokiVector(BaseModel):
    metric: Dict[str, str]
    value: Tuple[datetime, int]

    def __len__(self):
        return len(self.value)


class LokiStream(BaseModel):
    stream: Dict[str, str]
    values: List[Tuple[datetime, str]]

    def __len__(self):
        return len(self.values)


class LokiMatrix(BaseModel):
    metric: Dict[str, str]
    values: List[Tuple[datetime, int]]

    def __len__(self):
        return len(self.values)


class LokiStreamsData(LokiBaseData):
    resultType: str = Field(regex='^streams$')
    result: List[LokiStream]


class LokiMatrixData(LokiBaseData):
    resultType: str = Field(regex='^matrix$')
    result: List[LokiMatrix]


class LokiVectorData(LokiBaseData):
    resultType: str = Field(regex='^vector$')
    result: List[LokiVector]


class LokiListResponse(LokiBaseResponse):
    data: List[str] = []


class LokiStreamsResponse(LokiBaseResponse):
    data: LokiStreamsData


class LokiMatrixResponse(LokiBaseResponse):
    data: LokiMatrixData


class LokiVectorResponse(LokiBaseResponse):
    data: LokiVectorData
