from pydantic import BaseModel, HttpUrl, validator
from typing import Dict, Any, Optional, List, Union

class ProxyRequest(BaseModel):
    """代理请求模型"""
    target_url: str
    method: str
    headers: Optional[Dict[str, str]] = None
    params: Optional[Dict[str, Any]] = None
    data: Optional[Any] = None
    
    @validator('method')
    def validate_method(cls, v):
        """验证HTTP方法是否有效"""
        allowed_methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS']
        if v.upper() not in allowed_methods:
            raise ValueError(f'不支持的HTTP方法: {v}，允许的方法: {allowed_methods}')
        return v.upper()
    
    @validator('target_url')
    def validate_url(cls, v):
        """验证目标URL是否有效"""
        if not v.startswith(('http://', 'https://')):
            raise ValueError('目标URL必须以http://或https://开头')
        return v

class ProxyResponse(BaseModel):
    """代理响应模型"""
    status_code: int
    headers: Dict[str, str]
    data: Any
    response_time: float