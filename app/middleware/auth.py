from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, APIKeyHeader
from jose import JWTError, jwt
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import ipaddress

from app.core.config import settings

# OAuth2认证方案
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")

# API密钥认证方案
api_key_header = APIKeyHeader(name="X-API-Key")

# 模拟数据库中的API密钥（实际应用中应存储在数据库中）
API_KEYS = {
    "test-api-key": {"user_id": "test-user", "scopes": ["read", "write"]}
}

# 模拟数据库中的IP白名单（实际应用中应存储在数据库中）
IP_WHITELIST = [
    "127.0.0.1",
    "192.168.1.0/24",
]

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """创建JWT访问令牌"""
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme)):
    """验证JWT令牌并获取当前用户"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="无效的认证凭据",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # 解码JWT令牌
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        user_id: str = payload.get("sub")
        
        if user_id is None:
            raise credentials_exception
        
        # 这里应该从数据库获取用户信息
        # 为简化示例，直接返回用户ID
        return {"user_id": user_id}
    
    except JWTError:
        raise credentials_exception

async def verify_api_key(api_key: str = Depends(api_key_header)):
    """验证API密钥"""
    if api_key not in API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效的API密钥"
        )
    
    return API_KEYS[api_key]

async def verify_ip_whitelist(request: Request):
    """验证IP白名单"""
    client_ip = request.client.host
    
    # 检查IP是否在白名单中
    if client_ip in IP_WHITELIST:
        return True
    
    # 检查IP是否在CIDR范围内
    for ip_range in IP_WHITELIST:
        if "/" in ip_range:
            try:
                network = ipaddress.ip_network(ip_range)
                if ipaddress.ip_address(client_ip) in network:
                    return True
            except ValueError:
                continue
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="IP地址不在白名单中"
    )

# 组合认证依赖项
async def get_current_user_with_api_key(user: dict = Depends(get_current_user), api_key: dict = Depends(verify_api_key)):
    """同时验证JWT令牌和API密钥"""
    return {"user": user, "api_key": api_key}