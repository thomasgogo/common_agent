import logging
import time
import subprocess
import redis
import pymysql
import pymongo
import elasticsearch
import pika
from typing import Dict, Any, Optional, Tuple
from django.utils import timezone
from django.conf import settings

# 配置日志
logger = logging.getLogger(__name__)

# 定义重试装饰器
def retry(max_attempts=3, delay=2, backoff=2, exceptions=(Exception,)):
    """
    重试装饰器，用于在操作失败时进行重试
    
    Args:
        max_attempts: 最大尝试次数
        delay: 初始延迟时间（秒）
        backoff: 延迟时间的增长因子
        exceptions: 需要捕获的异常类型
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.error(f"操作失败，已达到最大重试次数: {str(e)}")
                        raise
                    
                    logger.warning(f"操作失败，将在 {current_delay} 秒后重试: {str(e)}")
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator


class MiddlewareAdapter:
    """中间件适配器基类"""
    
    def __init__(self, middleware):
        """
        初始化适配器
        
        Args:
            middleware: 中间件对象
        """
        self.middleware = middleware
        self.config = self._get_config()
    
    def _get_config(self):
        """获取中间件配置"""
        try:
            return self.middleware.config.config_data
        except Exception:
            return {}
    
    def start(self) -> Dict[str, Any]:
        """启动中间件服务"""
        raise NotImplementedError("子类必须实现此方法")
    
    def stop(self) -> Dict[str, Any]:
        """停止中间件服务"""
        raise NotImplementedError("子类必须实现此方法")
    
    def restart(self) -> Dict[str, Any]:
        """重启中间件服务"""
        self.stop()
        return self.start()
    
    def upgrade(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """升级中间件服务"""
        raise NotImplementedError("子类必须实现此方法")
    
    def update_config(self, new_config: Dict[str, Any]) -> Dict[str, Any]:
        """更新中间件配置"""
        raise NotImplementedError("子类必须实现此方法")
    
    def validate_config(self, config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """验证配置有效性
        
        Args:
            config: 要验证的配置字典
            
        Returns:
            (是否有效, 错误信息)
        """
        # 导入配置验证器
        from .config_validator import ConfigValidator
        
        # 获取中间件类型
        middleware_type = getattr(self.middleware, 'type', None)
        if not middleware_type:
            return False, "无法确定中间件类型"
        
        # 使用配置验证器验证配置
        validator = ConfigValidator()
        result = validator.validate_config(middleware_type, config)
        
        if not result.is_valid:
            # 如果有错误，返回第一个错误信息
            if result.errors:
                return False, result.errors[0]
            return False, "配置验证失败"
        
        # 如果有警告，记录到日志
        if result.warnings:
            for warning in result.warnings:
                logger.warning(f"配置警告: {warning}")
        
        return True, None
    
    def get_status(self) -> Dict[str, Any]:
        """获取中间件状态"""
        raise NotImplementedError("子类必须实现此方法")
    
    def backup(self, backup_path: Optional[str] = None) -> Dict[str, Any]:
        """备份中间件数据"""
        raise NotImplementedError("子类必须实现此方法")
    
    def restore(self, backup_path: str) -> Dict[str, Any]:
        """从备份恢复中间件数据"""
        raise NotImplementedError("子类必须实现此方法")


class RedisAdapter(MiddlewareAdapter):
    """Redis中间件适配器"""
    
    @retry(max_attempts=3, delay=2, exceptions=(redis.RedisError, ConnectionError))
    def _get_client(self):
        """获取Redis客户端连接"""
        return redis.Redis(
            host=self.middleware.host,
            port=self.middleware.port,
            db=self.config.get('db', 0),
            password=self.config.get('password'),
            socket_timeout=self.config.get('timeout', 5),
            socket_connect_timeout=self.config.get('connect_timeout', 5),
            decode_responses=True
        )
    
    def _execute_command(self, command: str, *args) -> str:
        """执行Redis命令行命令"""
        cmd = ["redis-cli"]
        
        if self.middleware.host != 'localhost' and self.middleware.host != '127.0.0.1':
            cmd.extend(["-h", self.middleware.host])
        
        cmd.extend(["-p", str(self.middleware.port)])
        
        if self.config.get('password'):
            cmd.extend(["-a", self.config.get('password')])
        
        cmd.append(command)
        cmd.extend([str(arg) for arg in args])
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                raise Exception(f"Redis命令执行失败: {result.stderr}")
            return result.stdout
        except subprocess.TimeoutExpired:
            raise Exception("Redis命令执行超时")
    
    @retry(max_attempts=3, delay=2, exceptions=(redis.RedisError, ConnectionError, Exception))
    def start(self) -> Dict[str, Any]:
        """启动Redis服务"""
        logger.info(f"正在启动Redis中间件: {self.middleware.id}")
        
        # 检查是否使用Docker
        if self.config.get('use_docker', False):
            container_name = self.config.get('container_name', f"redis-{self.middleware.id}")
            image = self.config.get('docker_image', 'redis:latest')
            
            # 检查容器是否存在
            check_cmd = ["docker", "ps", "-a", "-q", "-f", f"name={container_name}"]
            result = subprocess.run(check_cmd, capture_output=True, text=True)
            
            if result.stdout.strip():
                # 容器存在，启动它
                start_cmd = ["docker", "start", container_name]
                subprocess.run(start_cmd, check=True)
            else:
                # 容器不存在，创建并启动
                port_mapping = f"{self.middleware.port}:6379"
                run_cmd = [
                    "docker", "run", "-d",
                    "--name", container_name,
                    "-p", port_mapping
                ]
                
                # 添加密码配置
                if self.config.get('password'):
                    run_cmd.extend(["--requirepass", self.config.get('password')])
                
                # 添加持久化目录映射
                if self.config.get('data_dir'):
                    volume = f"{self.config.get('data_dir')}:/data"
                    run_cmd.extend(["-v", volume])
                
                run_cmd.append(image)
                subprocess.run(run_cmd, check=True)
        else:
            # 非Docker方式，使用系统服务
            # 这里假设使用systemd管理Redis服务
            service_name = self.config.get('service_name', 'redis-server')
            subprocess.run(["systemctl", "start", service_name], check=True)
        
        # 验证服务是否成功启动
        client = self._get_client()
        info = client.info()
        client.close()
        
        # 更新中间件状态
        self.middleware.status = 'running'
        self.middleware.last_updated = timezone.now()
        self.middleware.save()
        
        logger.info(f"Redis中间件 {self.middleware.id} 已成功启动")
        return {"success": True, "info": info}
    
    @retry(max_attempts=3, delay=2, exceptions=(redis.RedisError, ConnectionError, Exception))
    def stop(self) -> Dict[str, Any]:
        """停止Redis服务"""
        logger.info(f"正在停止Redis中间件: {self.middleware.id}")
        
        # 检查是否使用Docker
        if self.config.get('use_docker', False):
            container_name = self.config.get('container_name', f"redis-{self.middleware.id}")
            subprocess.run(["docker", "stop", container_name], check=True)
        else:
            # 非Docker方式，使用系统服务
            service_name = self.config.get('service_name', 'redis-server')
            subprocess.run(["systemctl", "stop", service_name], check=True)
        
        # 更新中间件状态
        self.middleware.status = 'stopped'
        self.middleware.last_updated = timezone.now()
        self.middleware.save()
        
        logger.info(f"Redis中间件 {self.middleware.id} 已成功停止")
        return {"success": True}
    
    @retry(max_attempts=3, delay=2, exceptions=(redis.RedisError, ConnectionError, Exception))
    def upgrade(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """升级Redis服务"""
        target_version = params.get("target_version")
        backup = params.get("backup", True)
        force = params.get("force", False)
        
        logger.info(f"正在升级Redis中间件 {self.middleware.id} 到版本 {target_version}")
        
        # 更新中间件状态为更新中
        self.middleware.status = 'updating'
        self.middleware.last_updated = timezone.now()
        self.middleware.save()
        
        # 如果需要备份，先进行备份
        backup_path = None
        if backup:
            backup_result = self.backup()
            backup_path = backup_result.get('backup_path')
            logger.info(f"已备份Redis中间件 {self.middleware.id} 到 {backup_path}")
        
        try:
            # 检查是否使用Docker
            if self.config.get('use_docker', False):
                container_name = self.config.get('container_name', f"redis-{self.middleware.id}")
                image = f"redis:{target_version}"
                
                # 停止并删除旧容器
                subprocess.run(["docker", "stop", container_name], check=True)
                subprocess.run(["docker", "rm", container_name], check=True)
                
                # 拉取新版本镜像
                subprocess.run(["docker", "pull", image], check=True)
                
                # 创建并启动新容器
                port_mapping = f"{self.middleware.port}:6379"
                run_cmd = [
                    "docker", "run", "-d",
                    "--name", container_name,
                    "-p", port_mapping
                ]
                
                # 添加密码配置
                if self.config.get('password'):
                    run_cmd.extend(["--requirepass", self.config.get('password')])
                
                # 添加持久化目录映射
                if self.config.get('data_dir'):
                    volume = f"{self.config.get('data_dir')}:/data"
                    run_cmd.extend(["-v", volume])
                
                run_cmd.append(image)
                subprocess.run(run_cmd, check=True)
            else:
                # 非Docker方式，使用系统包管理器升级
                # 这里假设使用apt作为包管理器
                subprocess.run(["apt-get", "update"], check=True)
                subprocess.run(["apt-get", "install", "-y", f"redis-server={target_version}*"], check=True)
                
                # 重启服务
                service_name = self.config.get('service_name', 'redis-server')
                subprocess.run(["systemctl", "restart", service_name], check=True)
            
            # 验证服务是否成功启动
            client = self._get_client()
            info = client.info()
            client.close()
            
            # 更新中间件版本和状态
            self.middleware.version = target_version
            self.middleware.status = 'running'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            logger.info(f"Redis中间件 {self.middleware.id} 已成功升级到版本 {target_version}")
            return {"success": True, "version": target_version}
            
        except Exception as e:
            logger.error(f"升级Redis中间件 {self.middleware.id} 失败: {str(e)}")
            
            # 如果有备份且升级失败，尝试恢复
            if backup_path:
                try:
                    logger.info(f"尝试从备份 {backup_path} 恢复Redis中间件 {self.middleware.id}")
                    self.restore(backup_path)
                except Exception as restore_error:
                    logger.error(f"恢复Redis中间件 {self.middleware.id} 失败: {str(restore_error)}")
            
            # 更新中间件状态为错误
            self.middleware.status = 'error'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            raise Exception(f"升级Redis中间件失败: {str(e)}")
    
    def validate_config(self, config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """验证Redis配置有效性"""
        logger.info(f"正在验证Redis中间件 {self.middleware.id} 的配置")
        
        # 验证必要的配置项
        if config.get('use_docker', False):
            # Docker模式下的配置验证
            if 'docker_image' in config and not config['docker_image']:
                return False, "Docker镜像名称不能为空"
        else:
            # 非Docker模式下的配置验证
            if 'service_name' in config and not config['service_name']:
                return False, "服务名称不能为空"
        
        # 验证端口范围
        if 'port' in config:
            port = config['port']
            if not isinstance(port, int) or port < 1 or port > 65535:
                return False, "端口号必须是1-65535之间的整数"
        
        # 验证数据库索引
        if 'db' in config:
            db = config['db']
            if not isinstance(db, int) or db < 0 or db > 15:
                return False, "Redis数据库索引必须是0-15之间的整数"
        
        # 验证超时设置
        for timeout_key in ['timeout', 'connect_timeout']:
            if timeout_key in config:
                timeout = config[timeout_key]
                if not isinstance(timeout, (int, float)) or timeout <= 0:
                    return False, f"{timeout_key}必须是正数"
        
        # 验证密码格式（如果有）
        if 'password' in config and config['password'] is not None:
            password = config['password']
            if not isinstance(password, str):
                return False, "密码必须是字符串类型"
            if len(password) > 0 and len(password) < 8:
                return False, "密码长度不能少于8个字符"
        
        # 验证数据目录（如果有）
        if 'data_dir' in config and config['data_dir']:
            import os
            data_dir = config['data_dir']
            if not os.path.isdir(data_dir):
                try:
                    # 尝试创建目录
                    os.makedirs(data_dir, exist_ok=True)
                except Exception as e:
                    return False, f"数据目录无效或无法创建: {str(e)}"
        
        logger.info(f"Redis中间件 {self.middleware.id} 配置验证通过")
        return True, None
    
    @retry(max_attempts=3, delay=2, exceptions=(redis.RedisError, ConnectionError, Exception))
    def update_config(self, new_config: Dict[str, Any]) -> Dict[str, Any]:
        """更新Redis配置"""
        logger.info(f"正在更新Redis中间件 {self.middleware.id} 的配置")
        
        # 首先验证新配置的有效性
        is_valid, error_message = self.validate_config(new_config)
        if not is_valid:
            raise ValueError(f"无效的配置: {error_message}")
        
        # 备份当前配置
        old_config = self.config.copy()
        
        try:
            # 更新配置
            self.config.update(new_config)
            
            # 检查是否使用Docker
            if self.config.get('use_docker', False):
                container_name = self.config.get('container_name', f"redis-{self.middleware.id}")
                
                # 对于Docker容器，某些配置需要重新创建容器才能生效
                # 这里仅处理可以热更新的配置
                
                # 例如，更新最大内存限制
                if 'maxmemory' in new_config:
                    maxmemory = new_config.get('maxmemory')
                    config_cmd = [
                        "docker", "exec", container_name,
                        "redis-cli"
                    ]
                    
                    if self.config.get('password'):
                        config_cmd.extend(["-a", self.config.get('password')])
                    
                    config_cmd.extend(["CONFIG", "SET", "maxmemory", str(maxmemory)])
                    subprocess.run(config_cmd, check=True)
                
                # 更新其他可热更新的参数
                if 'timeout' in new_config:
                    timeout = new_config.get('timeout')
                    config_cmd = [
                        "docker", "exec", container_name,
                        "redis-cli"
                    ]
                    
                    if self.config.get('password'):
                        config_cmd.extend(["-a", self.config.get('password')])
                    
                    config_cmd.extend(["CONFIG", "SET", "timeout", str(timeout)])
                    subprocess.run(config_cmd, check=True)
            else:
                # 非Docker方式，直接使用redis-cli更新配置
                # 这里仅处理可以热更新的配置
                
                # 例如，更新最大内存限制
                if 'maxmemory' in new_config:
                    maxmemory = new_config.get('maxmemory')
                    self._execute_command("CONFIG", "SET", "maxmemory", str(maxmemory))
                
                # 更新其他可热更新的参数
                if 'timeout' in new_config:
                    timeout = new_config.get('timeout')
                    self._execute_command("CONFIG", "SET", "timeout", str(timeout))
            
            # 更新中间件配置记录
            self.middleware.config.config_data.update(new_config)
            self.middleware.config.updated_at = timezone.now()
            self.middleware.config.save()
            
            # 更新中间件最后更新时间
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            logger.info(f"Redis中间件 {self.middleware.id} 配置已更新")
            return {"success": True, "config_updated": True}
            
        except Exception as e:
            logger.error(f"更新Redis中间件 {self.middleware.id} 配置失败: {str(e)}")
            
            # 恢复旧配置
            self.config = old_config
            
            # 更新中间件状态为错误
            self.middleware.status = 'error'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            raise Exception(f"更新Redis中间件配置失败: {str(e)}")
    
    @retry(max_attempts=3, delay=2, exceptions=(redis.RedisError, ConnectionError, Exception))
    def get_status(self) -> Dict[str, Any]:
        """获取Redis状态"""
        logger.info(f"正在获取Redis中间件 {self.middleware.id} 的状态")
        
        try:
            client = self._get_client()
            info = client.info()
            
            # 获取内存使用情况
            memory_info = client.info('memory')
            
            # 获取客户端连接信息
            clients_info = client.info('clients')
            
            # 获取统计信息
            stats_info = client.info('stats')
            
            # 获取复制信息
            replication_info = client.info('replication')
            
            # 获取持久化信息
            persistence_info = client.info('persistence')
            
            # 获取服务器信息
            server_info = client.info('server')
            
            # 获取键空间信息
            keyspace_info = client.info('keyspace')
            
            # 获取集群信息（如果是集群模式）
            cluster_info = {}
            try:
                if server_info.get('redis_mode') == 'cluster':
                    cluster_info = client.cluster_info()
            except Exception as cluster_error:
                logger.warning(f"获取集群信息失败: {str(cluster_error)}")
            
            # 获取慢日志信息
            slowlog_info = []
            try:
                slowlog_entries = client.slowlog_get(10)  # 获取最近10条慢日志
                for entry in slowlog_entries:
                    slowlog_info.append({
                        "id": entry['id'],
                        "timestamp": entry['start_time'],
                        "execution_time": entry['duration'],
                        "command": ' '.join(entry['command']),
                    })
            except Exception as slowlog_error:
                logger.warning(f"获取慢日志信息失败: {str(slowlog_error)}")
            
            # 关闭连接
            client.close()
            
            # 构建状态信息
            status_info = {
                "status": self.middleware.status,
                "version": server_info.get('redis_version'),
                "uptime_in_seconds": server_info.get('uptime_in_seconds'),
                "connected_clients": clients_info.get('connected_clients'),
                "maxclients": server_info.get('maxclients'),
                "used_memory_human": memory_info.get('used_memory_human'),
                "used_memory_peak_human": memory_info.get('used_memory_peak_human'),
                "maxmemory_human": memory_info.get('maxmemory_human', '0'),
                "total_commands_processed": stats_info.get('total_commands_processed'),
                "instantaneous_ops_per_sec": stats_info.get('instantaneous_ops_per_sec'),
                "rejected_connections": stats_info.get('rejected_connections'),
                "role": replication_info.get('role'),
                "connected_slaves": replication_info.get('connected_slaves', 0),
                "rdb_last_save_time": persistence_info.get('rdb_last_save_time'),
                "rdb_last_bgsave_status": persistence_info.get('rdb_last_bgsave_status'),
                "aof_enabled": persistence_info.get('aof_enabled'),
                "aof_last_rewrite_status": persistence_info.get('aof_last_rewrite_status') if persistence_info.get('aof_enabled') else None,
                "total_keys": sum(int(db_info.get('keys', 0)) for db_name, db_info in keyspace_info.items()) if keyspace_info else 0,
                "keyspace_info": keyspace_info,
                "cluster_enabled": server_info.get('redis_mode') == 'cluster',
                "cluster_info": cluster_info,
                "slowlog_info": slowlog_info,
                "last_checked": timezone.now().isoformat(),
                "cpu_usage": stats_info.get('used_cpu_sys', 0) + stats_info.get('used_cpu_user', 0)
            }
            
            # 添加健康状态评估
            health_issues = []
            
            # 检查内存使用率
            if memory_info.get('maxmemory', 0) > 0:
                memory_usage = memory_info.get('used_memory', 0) / memory_info.get('maxmemory', 1) * 100
                if memory_usage > 90:
                    health_issues.append(f"内存使用率过高: {memory_usage:.1f}%")
                elif memory_usage > 70:
                    health_issues.append(f"内存使用率较高: {memory_usage:.1f}%")
            
            # 检查连接数
            if server_info.get('maxclients', 0) > 0:
                connection_usage = clients_info.get('connected_clients', 0) / server_info.get('maxclients', 1) * 100
                if connection_usage > 90:
                    health_issues.append(f"连接数使用率过高: {connection_usage:.1f}%")
                elif connection_usage > 70:
                    health_issues.append(f"连接数使用率较高: {connection_usage:.1f}%")
            
            # 检查持久化状态
            if persistence_info.get('rdb_last_bgsave_status') != 'ok':
                health_issues.append(f"RDB持久化状态异常: {persistence_info.get('rdb_last_bgsave_status')}")
            
            if persistence_info.get('aof_enabled') and persistence_info.get('aof_last_rewrite_status') != 'ok':
                health_issues.append(f"AOF持久化状态异常: {persistence_info.get('aof_last_rewrite_status')}")
            
            status_info['health_issues'] = health_issues
            status_info['health_status'] = 'healthy' if not health_issues else 'warning'
            
            logger.info(f"已获取Redis中间件 {self.middleware.id} 的状态信息")
            return {"success": True, "status": status_info}
            
        except Exception as e:
            logger.error(f"获取Redis中间件 {self.middleware.id} 状态失败: {str(e)}")
            
            # 更新中间件状态为错误
            self.middleware.status = 'error'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            # 记录详细错误信息
            from .error_handler import ErrorTracker
            error_tracker = ErrorTracker()
            error_tracker.log_error(
                middleware_id=self.middleware.id,
                operation="get_status",
                error=e,
                context={"host": self.middleware.host, "port": self.middleware.port}
            )
            
            return {
                "success": False, 
                "error": str(e),
                "status": {
                    "status": "error",
                    "last_checked": timezone.now().isoformat()
                }
            }
    
    @retry(max_attempts=3, delay=2, exceptions=(redis.RedisError, ConnectionError, Exception))
    def backup(self, backup_path: Optional[str] = None) -> Dict[str, Any]:
        """备份Redis数据"""
        import os
        import shutil
        from datetime import datetime
        
        logger.info(f"正在备份Redis中间件 {self.middleware.id} 的数据")
        
        # 如果未指定备份路径，则使用默认路径
        if not backup_path:
            backup_dir = os.path.join(settings.BACKUP_DIR, 'redis', str(self.middleware.id))
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            backup_path = os.path.join(backup_dir, f"redis_backup_{timestamp}.rdb")
        
        try:
            # 获取Redis状态，确保服务正常运行
            status_result = self.get_status()
            if not status_result.get("success", False):
                raise Exception(f"无法获取Redis状态: {status_result.get('error', '未知错误')}")
            
            # 检查是否使用Docker
            if self.config.get('use_docker', False):
                container_name = self.config.get('container_name', f"redis-{self.middleware.id}")
                
                # 在容器内执行SAVE命令
                save_cmd = [
                    "docker", "exec", container_name,
                    "redis-cli"
                ]
                
                if self.config.get('password'):
                    save_cmd.extend(["-a", self.config.get('password')])
                
                save_cmd.append("SAVE")
                subprocess.run(save_cmd, check=True, capture_output=True, text=True)
                
                # 从容器中复制RDB文件
                copy_cmd = [
                    "docker", "cp",
                    f"{container_name}:/data/dump.rdb",
                    backup_path
                ]
                subprocess.run(copy_cmd, check=True, capture_output=True, text=True)
            else:
                # 非Docker方式，直接使用redis-cli执行SAVE命令
                save_result = self._execute_command("SAVE")
                logger.info(f"Redis SAVE命令执行结果: {save_result}")
                
                # 复制RDB文件
                redis_data_dir = self.config.get('data_dir', '/var/lib/redis')
                rdb_path = os.path.join(redis_data_dir, 'dump.rdb')
                
                if not os.path.exists(rdb_path):
                    raise FileNotFoundError(f"RDB文件不存在: {rdb_path}")
                
                shutil.copy2(rdb_path, backup_path)
            
            # 验证备份文件是否存在
            if not os.path.exists(backup_path):
                raise FileNotFoundError(f"备份文件创建失败: {backup_path}")
            
            # 记录备份信息
            backup_info = {
                "middleware_id": self.middleware.id,
                "backup_path": backup_path,
                "timestamp": datetime.now().isoformat(),
                "size": os.path.getsize(backup_path),
                "version": self.middleware.version
            }
            
            # 保存备份元数据
            metadata_path = f"{backup_path}.json"
            with open(metadata_path, 'w') as f:
                import json
                json.dump(backup_info, f, indent=2)
            
            logger.info(f"Redis中间件 {self.middleware.id} 数据已备份到 {backup_path}")
            return {"success": True, "backup_path": backup_path, "backup_info": backup_info}
            
        except Exception as e:
            logger.error(f"备份Redis中间件 {self.middleware.id} 数据失败: {str(e)}")
            
            # 记录详细错误信息
            from .error_handler import ErrorTracker
            error_tracker = ErrorTracker()
            error_tracker.log_error(
                middleware_id=self.middleware.id,
                operation="backup",
                error=e,
                context={"backup_path": backup_path}
            )
            
            return {"success": False, "error": str(e)}
    
    @retry(max_attempts=3, delay=2, exceptions=(redis.RedisError, ConnectionError, Exception))
    def restore(self, backup_path: str) -> Dict[str, Any]:
        """从备份恢复Redis数据"""
        import os
        import shutil
        import json
        from datetime import datetime
        
        logger.info(f"正在从 {backup_path} 恢复Redis中间件 {self.middleware.id} 的数据")
        
        # 检查备份文件是否存在
        if not os.path.isfile(backup_path):
            error_msg = f"备份文件 {backup_path} 不存在"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        
        # 读取备份元数据（如果存在）
        metadata_path = f"{backup_path}.json"
        backup_info = None
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r') as f:
                    backup_info = json.load(f)
                logger.info(f"已读取备份元数据: {backup_info}")
            except Exception as e:
                logger.warning(f"读取备份元数据失败: {str(e)}")
        
        # 创建恢复前的快照，以便恢复失败时回滚
        snapshot_path = None
        try:
            # 创建临时备份目录
            temp_backup_dir = os.path.join(settings.BACKUP_DIR, 'redis', str(self.middleware.id), 'temp')
            os.makedirs(temp_backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            snapshot_path = os.path.join(temp_backup_dir, f"pre_restore_snapshot_{timestamp}.rdb")
            
            # 创建当前数据的快照
            snapshot_result = self.backup(snapshot_path)
            if not snapshot_result.get("success", False):
                logger.warning(f"创建恢复前快照失败: {snapshot_result.get('error', '未知错误')}")
                snapshot_path = None
            else:
                logger.info(f"已创建恢复前快照: {snapshot_path}")
        except Exception as e:
            logger.warning(f"创建恢复前快照失败: {str(e)}")
            snapshot_path = None
        
        try:
            # 停止Redis服务
            stop_result = self.stop()
            if not stop_result.get("success", False):
                raise Exception(f"停止Redis服务失败: {stop_result.get('error', '未知错误')}")
            
            # 检查是否使用Docker
            if self.config.get('use_docker', False):
                container_name = self.config.get('container_name', f"redis-{self.middleware.id}")
                
                # 复制RDB文件到容器
                copy_cmd = [
                    "docker", "cp",
                    backup_path,
                    f"{container_name}:/data/dump.rdb"
                ]
                subprocess.run(copy_cmd, check=True, capture_output=True, text=True)
                
                # 设置适当的权限
                chmod_cmd = [
                    "docker", "exec", container_name,
                    "chmod", "644", "/data/dump.rdb"
                ]
                subprocess.run(chmod_cmd, check=True, capture_output=True, text=True)
            else:
                # 非Docker方式，直接复制RDB文件
                redis_data_dir = self.config.get('data_dir', '/var/lib/redis')
                rdb_path = os.path.join(redis_data_dir, 'dump.rdb')
                
                # 确保目标目录存在
                os.makedirs(os.path.dirname(rdb_path), exist_ok=True)
                
                # 复制备份文件
                shutil.copy2(backup_path, rdb_path)
                
                # 设置适当的权限
                os.chmod(rdb_path, 0o644)
            
            # 启动Redis服务
            start_result = self.start()
            if not start_result.get("success", False):
                raise Exception(f"启动Redis服务失败: {start_result.get('error', '未知错误')}")
            
            # 验证恢复是否成功
            status_result = self.get_status()
            if not status_result.get("success", False):
                raise Exception(f"验证Redis状态失败: {status_result.get('error', '未知错误')}")
            
            # 记录恢复信息
            restore_info = {
                "middleware_id": self.middleware.id,
                "backup_path": backup_path,
                "restore_timestamp": datetime.now().isoformat(),
                "backup_info": backup_info
            }
            
            logger.info(f"Redis中间件 {self.middleware.id} 数据已从 {backup_path} 恢复")
            return {"success": True, "restore_info": restore_info}
            
        except Exception as e:
            logger.error(f"恢复Redis中间件 {self.middleware.id} 数据失败: {str(e)}")
            
            # 如果有快照且恢复失败，尝试回滚
            if snapshot_path:
                try:
                    logger.info(f"尝试回滚到恢复前的状态: {snapshot_path}")
                    
                    # 停止服务（如果正在运行）
                    try:
                        self.stop()
                    except Exception as stop_error:
                        logger.error(f"回滚时停止Redis服务失败: {str(stop_error)}")
                    
                    # 复制快照文件
                    if self.config.get('use_docker', False):
                        container_name = self.config.get('container_name', f"redis-{self.middleware.id}")
                        copy_cmd = ["docker", "cp", snapshot_path, f"{container_name}:/data/dump.rdb"]
                        subprocess.run(copy_cmd, check=True, capture_output=True, text=True)
                    else:
                        redis_data_dir = self.config.get('data_dir', '/var/lib/redis')
                        rdb_path = os.path.join(redis_data_dir, 'dump.rdb')
                        shutil.copy2(snapshot_path, rdb_path)
                    
                    # 启动服务
                    self.start()
                    logger.info("已成功回滚到恢复前的状态")
                except Exception as rollback_error:
                    logger.error(f"回滚失败: {str(rollback_error)}")
            
            # 尝试重新启动服务
            try:
                self.start()
            except Exception as start_error:
                logger.error(f"恢复后启动Redis中间件 {self.middleware.id} 失败: {str(start_error)}")
            
            # 更新中间件状态为错误
            self.middleware.status = 'error'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            # 记录详细错误信息
            from .error_handler import ErrorTracker
            error_tracker = ErrorTracker()
            error_tracker.log_error(
                middleware_id=self.middleware.id,
                operation="restore",
                error=e,
                context={"backup_path": backup_path, "snapshot_path": snapshot_path}
            )
            
            return {"success": False, "error": str(e)}