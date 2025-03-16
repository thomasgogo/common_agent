from pydantic import BaseModel, validator
from typing import Dict, Any, Optional, List, Union, Literal
from datetime import datetime


class MiddlewareBase(BaseModel):
    """中间件基础模型"""
    name: str
    type: str  # 中间件类型: redis, mysql, etc.
    host: str
    port: int
    version: str
    status: str  # running, stopped, updating, error
    last_updated: datetime = datetime.now()
    
    @validator('type')
    def validate_type(cls, v):
        """验证中间件类型是否支持"""
        allowed_types = ['redis', 'mysql', 'mongodb', 'elasticsearch', 'rabbitmq']
        if v.lower() not in allowed_types:
            raise ValueError(f'不支持的中间件类型: {v}，支持的类型: {allowed_types}')
        return v.lower()
    
    @validator('status')
    def validate_status(cls, v):
        """验证状态是否有效"""
        allowed_status = ['running', 'stopped', 'updating', 'error']
        if v.lower() not in allowed_status:
            raise ValueError(f'无效的状态: {v}，有效的状态: {allowed_status}')
        return v.lower()


class RedisConfig(BaseModel):
    """Redis配置模型"""
    host: str
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    max_connections: int = 10
    socket_timeout: int = 5
    socket_connect_timeout: int = 5


class MySQLConfig(BaseModel):
    """MySQL配置模型"""
    host: str
    port: int = 3306
    user: str
    password: str
    database: str
    charset: str = 'utf8mb4'
    max_connections: int = 5
    connection_timeout: int = 10


class MongoDBConfig(BaseModel):
    """MongoDB配置模型"""
    host: str
    port: int = 27017
    user: Optional[str] = None
    password: Optional[str] = None
    database: str
    auth_source: str = 'admin'
    max_pool_size: int = 5


class ElasticsearchConfig(BaseModel):
    """Elasticsearch配置模型"""
    hosts: List[str]
    username: Optional[str] = None
    password: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3
    retry_on_timeout: bool = True


class RabbitMQConfig(BaseModel):
    """RabbitMQ配置模型"""
    host: str
    port: int = 5672
    virtual_host: str = '/'
    username: str
    password: str
    connection_attempts: int = 3
    retry_delay: int = 5


class MiddlewareConfig(BaseModel):
    """中间件配置模型，根据类型包含不同的配置"""
    type: str
    config: Union[RedisConfig, MySQLConfig, MongoDBConfig, ElasticsearchConfig, RabbitMQConfig]
    
    @validator('config', pre=True)
    def validate_config_type(cls, v, values):
        """验证配置类型与中间件类型是否匹配"""
        middleware_type = values.get('type', '').lower()
        if middleware_type == 'redis' and not isinstance(v, (dict, RedisConfig)):
            raise ValueError('Redis中间件必须使用RedisConfig配置')
        elif middleware_type == 'mysql' and not isinstance(v, (dict, MySQLConfig)):
            raise ValueError('MySQL中间件必须使用MySQLConfig配置')
        elif middleware_type == 'mongodb' and not isinstance(v, (dict, MongoDBConfig)):
            raise ValueError('MongoDB中间件必须使用MongoDBConfig配置')
        elif middleware_type == 'elasticsearch' and not isinstance(v, (dict, ElasticsearchConfig)):
            raise ValueError('Elasticsearch中间件必须使用ElasticsearchConfig配置')
        elif middleware_type == 'rabbitmq' and not isinstance(v, (dict, RabbitMQConfig)):
            raise ValueError('RabbitMQ中间件必须使用RabbitMQConfig配置')
        return v


class MiddlewareOperation(BaseModel):
    """中间件操作模型"""
    operation_id: str
    middleware_id: str
    operation_type: Literal['start', 'stop', 'restart', 'update', 'upgrade', 'config_update']
    status: Literal['pending', 'in_progress', 'completed', 'failed']
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    params: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


class MiddlewareStatus(BaseModel):
    """中间件状态模型"""
    middleware_id: str
    status: Literal['running', 'stopped', 'updating', 'error']
    version: str
    uptime: Optional[int] = None  # 运行时间（秒）
    connections: Optional[int] = None  # 当前连接数
    memory_usage: Optional[float] = None  # 内存使用（MB）
    cpu_usage: Optional[float] = None  # CPU使用率（%）
    last_checked: datetime = datetime.now()


class MiddlewareUpgradeRequest(BaseModel):
    """中间件升级请求模型"""
    middleware_id: str
    target_version: str
    schedule_time: Optional[datetime] = None  # 计划升级时间，为空表示立即升级
    backup: bool = True  # 是否在升级前备份
    force: bool = False  # 是否强制升级（忽略兼容性检查）


class MiddlewareConfigUpdateRequest(BaseModel):
    """中间件配置更新请求模型"""
    middleware_id: str
    config: Dict[str, Any]  # 新的配置参数
    restart_after_update: bool = True  # 更新配置后是否重启服务
    validate_only: bool = False  # 是否仅验证配置而不应用