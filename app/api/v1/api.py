from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
import aiohttp
from typing import Dict, Any, Optional, List
import json
import time

from app.core.config import settings
from app.middleware.auth import get_current_user
from app.models.proxy import ProxyRequest, ProxyResponse

# 创建API路由器
api_router = APIRouter()

# 代理请求处理函数
async def proxy_request(target_url: str, method: str, headers: Dict, data: Any = None, params: Dict = None) -> Dict:
    """处理代理请求的核心函数"""
    async with aiohttp.ClientSession() as session:
        try:
            # 准备请求参数
            request_kwargs = {
                "headers": headers,
                "params": params,
            }
            
            if data:
                if isinstance(data, dict):
                    request_kwargs["json"] = data
                else:
                    request_kwargs["data"] = data
            
            # 发送请求
            start_time = time.time()
            async with getattr(session, method.lower())(target_url, **request_kwargs) as response:
                response_time = time.time() - start_time
                
                # 读取响应内容
                content_type = response.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    response_data = await response.json()
                else:
                    response_data = await response.text()
                
                # 构建响应对象
                return {
                    "status_code": response.status,
                    "headers": dict(response.headers),
                    "data": response_data,
                    "response_time": response_time
                }
        except aiohttp.ClientError as e:
            raise HTTPException(status_code=500, detail=f"代理请求失败: {str(e)}")

# 通用代理端点
@api_router.post("/proxy", response_model=ProxyResponse)
async def proxy(request: ProxyRequest, req: Request):
    """通用代理端点，转发请求到目标服务"""
    # 获取请求头，但移除一些不需要转发的头部
    headers = dict(req.headers)
    headers_to_remove = ["host", "content-length", "connection"]
    for header in headers_to_remove:
        if header in headers:
            del headers[header]
    
    # 如果有自定义头部，添加到请求中
    if request.headers:
        headers.update(request.headers)
    
    # 发送代理请求
    response_data = await proxy_request(
        target_url=request.target_url,
        method=request.method,
        headers=headers,
        data=request.data,
        params=request.params
    )
    
    # 返回响应
    return ProxyResponse(
        status_code=response_data["status_code"],
        headers=response_data["headers"],
        data=response_data["data"],
        response_time=response_data["response_time"]
    )

# 健康检查端点
@api_router.get("/health")
async def health_check():
    """健康检查端点"""
    return {"status": "ok", "version": "0.1.0"}

# 服务信息端点
@api_router.get("/info")
async def service_info():
    """获取服务信息"""
    return {
        "name": settings.PROJECT_NAME,
        "version": "0.1.0",
        "description": "通用型网关代理框架",
        "features": [
            "请求转发",
            "响应缓存",
            "限流控制",
            "认证授权"
        ]
    }