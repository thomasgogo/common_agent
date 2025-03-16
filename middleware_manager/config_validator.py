import logging
import json
import os
from typing import Dict, Any, Optional, Tuple, List, Set
from datetime import datetime
from pydantic import BaseModel, ValidationError

from app.models.middleware import (
    RedisConfig, 
    MySQLConfig, 
    MongoDBConfig, 
    ElasticsearchConfig, 
    RabbitMQConfig
)

# 配置日志
logger = logging.getLogger(__name__)


class ConfigValidationResult:
    """配置验证结果类"""
    
    def __init__(self, is_valid: bool, errors: Optional[List[str]] = None, warnings: Optional[List[str]] = None):
        self.is_valid = is_valid
        self.errors = errors or []
        self.warnings = warnings or []
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "timestamp": self.timestamp.isoformat()
        }
    
    def __bool__(self) -> bool:
        """允许直接在条件表达式中使用"""
        return self.is_valid


class ConfigVersionManager:
    """配置版本管理器，用于跟踪配置变更历史"""
    
    def __init__(self, history_dir: str = 'config_history'):
        self.history_dir = history_dir
        os.makedirs(history_dir, exist_ok=True)
    
    def save_config_version(self, middleware_id: str, config: Dict[str, Any]) -> str:
        """保存配置版本
        
        Args:
            middleware_id: 中间件ID
            config: 配置数据
            
        Returns:
            配置版本文件路径
        """
        middleware_dir = os.path.join(self.history_dir, middleware_id)
        os.makedirs(middleware_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        version_file = os.path.join(middleware_dir, f"config_{timestamp}.json")
        
        config_data = {
            "version_id": timestamp,
            "middleware_id": middleware_id,
            "timestamp": datetime.now().isoformat(),
            "config": config
        }
        
        with open(version_file, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        logger.info(f"已保存中间件 {middleware_id} 的配置版本 {timestamp}")
        return version_file
    
    def get_config_history(self, middleware_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """获取配置历史记录
        
        Args:
            middleware_id: 中间件ID
            limit: 返回的最大记录数
            
        Returns:
            配置历史记录列表
        """
        middleware_dir = os.path.join(self.history_dir, middleware_id)
        if not os.path.exists(middleware_dir):
            return []
        
        version_files = [f for f in os.listdir(middleware_dir) if f.endswith('.json')]
        version_files.sort(reverse=True)  # 按文件名倒序排序，最新的版本在前面
        
        history = []
        for file_name in version_files[:limit]:
            try:
                with open(os.path.join(middleware_dir, file_name), 'r') as f:
                    config_data = json.load(f)
                    history.append(config_data)
            except Exception as e:
                logger.warning(f"读取配置版本 {file_name} 失败: {str(e)}")
        
        return history
    
    def get_config_version(self, middleware_id: str, version_id: str) -> Optional[Dict[str, Any]]:
        """获取指定版本的配置
        
        Args:
            middleware_id: 中间件ID
            version_id: 版本ID
            
        Returns:
            配置数据，如果版本不存在则返回None
        """
        version_file = os.path.join(self.history_dir, middleware_id, f"config_{version_id}.json")
        if not os.path.exists(version_file):
            return None
        
        try:
            with open(version_file, 'r') as f:
                config_data = json.load(f)
                return config_data.get("config")
        except Exception as e:
            logger.error(f"读取配置版本 {version_id} 失败: {str(e)}")
            return None
    
    def compare_configs(self, old_config: Dict[str, Any], new_config: Dict[str, Any]) -> Dict[str, Any]:
        """比较两个配置版本的差异
        
        Args:
            old_config: 旧配置
            new_config: 新配置
            
        Returns:
            配置差异信息
        """
        # 获取所有键的集合
        all_keys = set(old_config.keys()) | set(new_config.keys())
        
        added = {k: new_config[k] for k in all_keys if k in new_config and k not in old_config}
        removed = {k: old_config[k] for k in all_keys if k in old_config and k not in new_config}
        modified = {k: {"old": old_config[k], "new": new_config[k]} 
                  for k in all_keys if k in old_config and k in new_config and old_config[k] != new_config[k]}
        
        return {
            "added": added,
            "removed": removed,
            "modified": modified,
            "unchanged": len(all_keys) - len(added) - len(removed) - len(modified)
        }


class ConfigValidator:
    """配置验证器，用于验证中间件配置的有效性"""
    
    def __init__(self):
        self.version_manager = ConfigVersionManager()
        self.config_models = {
            'redis': RedisConfig,
            'mysql': MySQLConfig,
            'mongodb': MongoDBConfig,
            'elasticsearch': ElasticsearchConfig,
            'rabbitmq': RabbitMQConfig
        }
    
    def validate_config(self, middleware_type: str, config: Dict[str, Any]) -> ConfigValidationResult:
        """验证配置有效性
        
        Args:
            middleware_type: 中间件类型
            config: 配置数据
            
        Returns:
            验证结果
        """
        logger.info(f"正在验证 {middleware_type} 中间件配置")
        
        # 检查中间件类型是否支持
        if middleware_type.lower() not in self.config_models:
            return ConfigValidationResult(False, [f"不支持的中间件类型: {middleware_type}"])
        
        # 获取对应的配置模型
        config_model = self.config_models[middleware_type.lower()]
        
        # 验证配置
        try:
            # 使用Pydantic模型验证配置
            validated_config = config_model(**config)
            return ConfigValidationResult(True)
        except ValidationError as e:
            # 提取验证错误信息
            errors = [f"{error['loc'][0]}: {error['msg']}" for error in json.loads(e.json())]
            return ConfigValidationResult(False, errors)
    
    def validate_config_change(self, middleware_id: str, middleware_type: str, 
                              old_config: Dict[str, Any], new_config: Dict[str, Any]) -> ConfigValidationResult:
        """验证配置变更的有效性和安全性
        
        Args:
            middleware_id: 中间件ID
            middleware_type: 中间件类型
            old_config: 旧配置
            new_config: 新配置
            
        Returns:
            验证结果
        """
        logger.info(f"正在验证中间件 {middleware_id} 的配置变更")
        
        # 首先验证新配置的基本有效性
        basic_validation = self.validate_config(middleware_type, new_config)
        if not basic_validation.is_valid:
            return basic_validation
        
        # 比较配置差异
        diff = self.version_manager.compare_configs(old_config, new_config)
        
        # 检查敏感配置项的变更
        warnings = []
        errors = []
        
        # 定义敏感配置项
        sensitive_configs = {
            'redis': {'port', 'password', 'data_dir'},
            'mysql': {'port', 'user', 'password', 'database', 'data_dir'},
            'mongodb': {'port', 'user', 'password', 'database', 'data_dir'},
            'elasticsearch': {'hosts', 'username', 'password'},
            'rabbitmq': {'port', 'username', 'password', 'virtual_host'}
        }
        
        # 获取当前中间件类型的敏感配置项
        sensitive_keys = sensitive_configs.get(middleware_type.lower(), set())
        
        # 检查敏感配置项的变更
        for key in diff['modified']:
            if key in sensitive_keys:
                warnings.append(f"敏感配置项 {key} 已被修改")
        
        for key in diff['removed']:
            if key in sensitive_keys:
                errors.append(f"敏感配置项 {key} 不能被删除")
        
        # 特定中间件类型的验证逻辑
        if middleware_type.lower() == 'redis':
            # Redis特定验证
            if 'maxmemory' in diff['modified'] and diff['modified']['maxmemory']['new'] < diff['modified']['maxmemory']['old']:
                warnings.append(f"Redis最大内存配置已减小，可能导致性能问题")
            
            # 检查持久化配置
            if 'save' in diff['removed']:
                warnings.append("Redis持久化配置已被移除，可能导致数据丢失")
            
            # 检查连接数限制
            if 'max_connections' in diff['modified'] and diff['modified']['max_connections']['new'] < diff['modified']['max_connections']['old']:
                warnings.append("Redis最大连接数已减小，可能导致连接拒绝")
        
        elif middleware_type.lower() == 'mysql':
            # MySQL特定验证
            if 'max_connections' in diff['modified'] and diff['modified']['max_connections']['new'] < diff['modified']['max_connections']['old']:
                warnings.append(f"MySQL最大连接数已减小，可能导致连接拒绝")
            
            # 检查缓冲区大小
            if 'innodb_buffer_pool_size' in diff['modified'] and diff['modified']['innodb_buffer_pool_size']['new'] < diff['modified']['innodb_buffer_pool_size']['old']:
                warnings.append("InnoDB缓冲池大小已减小，可能影响性能")
        
        elif middleware_type.lower() == 'mongodb':
            # MongoDB特定验证
            if 'max_pool_size' in diff['modified'] and diff['modified']['max_pool_size']['new'] < diff['modified']['max_pool_size']['old']:
                warnings.append("MongoDB连接池大小已减小，可能影响并发性能")
        
        elif middleware_type.lower() == 'elasticsearch':
            # Elasticsearch特定验证
            if 'cluster.name' in diff['modified']:
                errors.append("不允许修改Elasticsearch集群名称，这可能导致节点无法加入集群")
        
        # 保存配置变更记录
        self.version_manager.save_config_version(middleware_id, new_config)
        
        # 返回验证结果
        if errors:
            return ConfigValidationResult(False, errors, warnings)
        else:
            return ConfigValidationResult(True, [], warnings)
    
    def get_safe_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """获取安全的配置（移除敏感信息）
        
        Args:
            config: 原始配置
            
        Returns:
            安全的配置
        """
        # 复制配置以避免修改原始数据
        safe_config = config.copy()
        
        # 移除敏感信息
        sensitive_keys = {'password', 'secret', 'key', 'token', 'auth', 'credential'}
        for key in list(safe_config.keys()):
            if any(sensitive in key.lower() for sensitive in sensitive_keys):
                safe_config[key] = '******'
        
        return safe_config
    
    def rollback_config(self, middleware_id: str, version_id: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """回滚配置到指定版本
        
        Args:
            middleware_id: 中间件ID
            version_id: 版本ID
            
        Returns:
            (成功标志, 回滚后的配置, 错误信息)
        """
        logger.info(f"正在将中间件 {middleware_id} 的配置回滚到版本 {version_id}")
        
        # 获取指定版本的配置
        config = self.version_manager.get_config_version(middleware_id, version_id)
        if not config:
            return False, None, f"版本 {version_id} 不存在"
        
        # 保存回滚操作的记录
        rollback_note = {
            "rollback": True,
            "from_version": version_id,
            "timestamp": datetime.now().isoformat()
        }
        config["_rollback_info"] = rollback_note
        
        # 保存为新版本
        self.version_manager.save_config_version(middleware_id, config)
        
        return True, config, None


# 使用示例
def example_usage():
    # 创建配置验证器
    validator = ConfigValidator()
    
    # 验证Redis配置
    redis_config = {
        "host": "localhost",
        "port": 6379,
        "db": 0,
        "password": "secret123",
        "max_connections": 10
    }
    
    result = validator.validate_config("redis", redis_config)
    print(f"Redis配置验证结果: {result.to_dict()}")
    
    # 验证配置变更
    old_config = redis_config.copy()
    new_config = redis_config.copy()
    new_config["port"] = 6380
    new_config["max_connections"] = 5
    
    change_result = validator.validate_config_change("redis-test", "redis", old_config, new_config)
    print(f"配置变更验证结果: {change_result.to_dict()}")
    
    # 获取安全配置
    safe_config = validator.get_safe_config(redis_config)
    print(f"安全配置: {safe_config}")
    
    # 回滚配置示例
    success, config, error = validator.rollback_config("redis-test", "20230101000000")
    if success:
        print(f"配置已回滚: {config}")
    else:
        print(f"回滚失败: {error}")


if __name__ == "__main__":
    example_usage()