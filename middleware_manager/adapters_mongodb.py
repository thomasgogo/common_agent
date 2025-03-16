import logging
import time
import subprocess
import pymongo
from typing import Dict, Any, Optional, Tuple
from django.utils import timezone
from django.conf import settings
import os
import shutil
from datetime import datetime
import json

# 导入基础适配器类
from .adapters import MiddlewareAdapter, retry

# 配置日志
logger = logging.getLogger(__name__)


class MongoDBAdapter(MiddlewareAdapter):
    """MongoDB中间件适配器"""
    
    @retry(max_attempts=3, delay=2, exceptions=(pymongo.errors.PyMongoError, ConnectionError))
    def _get_client(self):
        """获取MongoDB客户端连接"""
        connection_params = {
            'host': self.middleware.host,
            'port': self.middleware.port,
            'maxPoolSize': self.config.get('max_pool_size', 5),
            'serverSelectionTimeoutMS': 5000,  # 5秒超时
            'connectTimeoutMS': 5000
        }
        
        # 添加认证信息
        if self.config.get('user') and self.config.get('password'):
            connection_params['username'] = self.config.get('user')
            connection_params['password'] = self.config.get('password')
            connection_params['authSource'] = self.config.get('auth_source', 'admin')
        
        return pymongo.MongoClient(**connection_params)
    
    def _execute_command(self, command: Dict[str, Any], db_name: str = 'admin') -> Dict[str, Any]:
        """执行MongoDB命令"""
        try:
            client = self._get_client()
            db = client[db_name]
            result = db.command(command)
            client.close()
            return {"success": True, "result": result}
        except Exception as e:
            logger.error(f"MongoDB命令执行失败: {str(e)}")
            return {"success": False, "error": str(e)}
    
    @retry(max_attempts=3, delay=2, exceptions=(pymongo.errors.PyMongoError, ConnectionError, Exception))
    def start(self) -> Dict[str, Any]:
        """启动MongoDB服务"""
        logger.info(f"正在启动MongoDB中间件: {self.middleware.id}")
        
        # 检查是否使用Docker
        if self.config.get('use_docker', False):
            container_name = self.config.get('container_name', f"mongodb-{self.middleware.id}")
            image = self.config.get('docker_image', 'mongo:latest')
            
            # 检查容器是否存在
            check_cmd = ["docker", "ps", "-a", "-q", "-f", f"name={container_name}"]
            result = subprocess.run(check_cmd, capture_output=True, text=True)
            
            if result.stdout.strip():
                # 容器存在，启动它
                start_cmd = ["docker", "start", container_name]
                subprocess.run(start_cmd, check=True)
            else:
                # 容器不存在，创建并启动
                port_mapping = f"{self.middleware.port}:27017"
                run_cmd = [
                    "docker", "run", "-d",
                    "--name", container_name,
                    "-p", port_mapping
                ]
                
                # 添加认证信息
                if self.config.get('user') and self.config.get('password'):
                    run_cmd.extend([
                        "-e", f"MONGO_INITDB_ROOT_USERNAME={self.config.get('user')}",
                        "-e", f"MONGO_INITDB_ROOT_PASSWORD={self.config.get('password')}"
                    ])
                
                # 添加数据库名称
                if self.config.get('database'):
                    run_cmd.extend(["-e", f"MONGO_INITDB_DATABASE={self.config.get('database')}"])
                
                # 添加持久化目录映射
                if self.config.get('data_dir'):
                    volume = f"{self.config.get('data_dir')}:/data/db"
                    run_cmd.extend(["-v", volume])
                
                # 添加自定义配置文件
                if self.config.get('config_file'):
                    config_volume = f"{self.config.get('config_file')}:/etc/mongod.conf.d/custom.conf"
                    run_cmd.extend(["-v", config_volume])
                
                run_cmd.append(image)
                subprocess.run(run_cmd, check=True)
        else:
            # 非Docker方式，使用系统服务
            # 这里假设使用systemd管理MongoDB服务
            service_name = self.config.get('service_name', 'mongod')
            subprocess.run(["systemctl", "start", service_name], check=True)
        
        # 等待服务启动
        max_retries = 10
        retry_interval = 3
        for i in range(max_retries):
            try:
                # 尝试连接数据库
                client = self._get_client()
                client.admin.command('ping')
                client.close()
                break
            except Exception as e:
                if i == max_retries - 1:
                    logger.error(f"MongoDB服务启动失败: {str(e)}")
                    raise
                logger.warning(f"等待MongoDB服务启动 ({i+1}/{max_retries}): {str(e)}")
                time.sleep(retry_interval)
        
        # 验证服务是否成功启动
        status_info = self._execute_command({"serverStatus": 1})
        if not status_info.get("success"):
            raise Exception(f"无法获取MongoDB状态信息: {status_info.get('error')}")
        
        # 更新中间件状态
        self.middleware.status = 'running'
        self.middleware.last_updated = timezone.now()
        self.middleware.save()
        
        logger.info(f"MongoDB中间件 {self.middleware.id} 已成功启动")
        return {"success": True, "info": status_info.get("result")}
    
    @retry(max_attempts=3, delay=2, exceptions=(pymongo.errors.PyMongoError, ConnectionError, Exception))
    def stop(self) -> Dict[str, Any]:
        """停止MongoDB服务"""
        logger.info(f"正在停止MongoDB中间件: {self.middleware.id}")
        
        # 检查是否使用Docker
        if self.config.get('use_docker', False):
            container_name = self.config.get('container_name', f"mongodb-{self.middleware.id}")
            subprocess.run(["docker", "stop", container_name], check=True)
        else:
            # 非Docker方式，使用系统服务
            service_name = self.config.get('service_name', 'mongod')
            subprocess.run(["systemctl", "stop", service_name], check=True)
        
        # 更新中间件状态
        self.middleware.status = 'stopped'
        self.middleware.last_updated = timezone.now()
        self.middleware.save()
        
        logger.info(f"MongoDB中间件 {self.middleware.id} 已成功停止")
        return {"success": True}
    
    @retry(max_attempts=3, delay=2, exceptions=(pymongo.errors.PyMongoError, ConnectionError, Exception))
    def upgrade(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """升级MongoDB服务"""
        target_version = params.get("target_version")
        backup = params.get("backup", True)
        force = params.get("force", False)
        
        logger.info(f"正在升级MongoDB中间件 {self.middleware.id} 到版本 {target_version}")
        
        # 更新中间件状态为更新中
        self.middleware.status = 'updating'
        self.middleware.last_updated = timezone.now()
        self.middleware.save()
        
        # 如果需要备份，先进行备份
        backup_path = None
        if backup:
            backup_result = self.backup()
            backup_path = backup_result.get('backup_path')
            logger.info(f"已备份MongoDB中间件 {self.middleware.id} 到 {backup_path}")
        
        try:
            # 检查是否使用Docker
            if self.config.get('use_docker', False):
                container_name = self.config.get('container_name', f"mongodb-{self.middleware.id}")
                image = f"mongo:{target_version}"
                
                # 停止并删除旧容器
                subprocess.run(["docker", "stop", container_name], check=True)
                subprocess.run(["docker", "rm", container_name], check=True)
                
                # 拉取新版本镜像
                subprocess.run(["docker", "pull", image], check=True)
                
                # 创建并启动新容器
                port_mapping = f"{self.middleware.port}:27017"
                run_cmd = [
                    "docker", "run", "-d",
                    "--name", container_name,
                    "-p", port_mapping
                ]
                
                # 添加认证信息
                if self.config.get('user') and self.config.get('password'):
                    run_cmd.extend([
                        "-e", f"MONGO_INITDB_ROOT_USERNAME={self.config.get('user')}",
                        "-e", f"MONGO_INITDB_ROOT_PASSWORD={self.config.get('password')}"
                    ])
                
                # 添加数据库名称
                if self.config.get('database'):
                    run_cmd.extend(["-e", f"MONGO_INITDB_DATABASE={self.config.get('database')}"])
                
                # 添加持久化目录映射
                if self.config.get('data_dir'):
                    volume = f"{self.config.get('data_dir')}:/data/db"
                    run_cmd.extend(["-v", volume])
                
                # 添加自定义配置文件
                if self.config.get('config_file'):
                    config_volume = f"{self.config.get('config_file')}:/etc/mongod.conf.d/custom.conf"
                    run_cmd.extend(["-v", config_volume])
                
                run_cmd.append(image)
                subprocess.run(run_cmd, check=True)
            else:
                # 非Docker方式，使用系统包管理器升级
                # 这里假设使用apt作为包管理器
                subprocess.run(["apt-get", "update"], check=True)
                subprocess.run(["apt-get", "install", "-y", f"mongodb-org={target_version}*"], check=True)
                
                # 重启服务
                service_name = self.config.get('service_name', 'mongod')
                subprocess.run(["systemctl", "restart", service_name], check=True)
            
            # 等待服务启动
            max_retries = 10
            retry_interval = 3
            for i in range(max_retries):
                try:
                    # 尝试连接数据库
                    client = self._get_client()
                    client.admin.command('ping')
                    client.close()
                    break
                except Exception as e:
                    if i == max_retries - 1:
                        logger.error(f"MongoDB服务启动失败: {str(e)}")
                        raise
                    logger.warning(f"等待MongoDB服务启动 ({i+1}/{max_retries}): {str(e)}")
                    time.sleep(retry_interval)
            
            # 验证服务是否成功启动
            status_info = self._execute_command({"serverStatus": 1})
            if not status_info.get("success"):
                raise Exception(f"无法获取MongoDB状态信息: {status_info.get('error')}")
            
            # 更新中间件版本和状态
            self.middleware.version = target_version
            self.middleware.status = 'running'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            logger.info(f"MongoDB中间件 {self.middleware.id} 已成功升级到版本 {target_version}")
            return {"success": True, "version": target_version}
            
        except Exception as e:
            logger.error(f"升级MongoDB中间件 {self.middleware.id} 失败: {str(e)}")
            
            # 如果有备份且升级失败，尝试恢复
            if backup_path:
                try:
                    logger.info(f"尝试从备份 {backup_path} 恢复MongoDB中间件 {self.middleware.id}")
                    self.restore(backup_path)
                except Exception as restore_error:
                    logger.error(f"恢复MongoDB中间件 {self.middleware.id} 失败: {str(restore_error)}")
            
            # 更新中间件状态为错误
            self.middleware.status = 'error'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            raise
    
    @retry(max_attempts=3, delay=2, exceptions=(pymongo.errors.PyMongoError, ConnectionError, Exception))
    def get_status(self) -> Dict[str, Any]:
        """获取MongoDB状态信息"""
        logger.info(f"获取MongoDB中间件 {self.middleware.id} 状态信息")
        
        try:
            # 检查服务是否运行
            if self.middleware.status != 'running':
                return {
                    "status": self.middleware.status,
                    "version": self.middleware.version,
                    "uptime": 0,
                    "connections": 0,
                    "memory_usage": 0,
                    "cpu_usage": 0
                }
            
            # 获取MongoDB状态信息
            status_info = self._execute_command({"serverStatus": 1})
            if not status_info.get("success"):
                raise Exception(f"无法获取MongoDB状态信息: {status_info.get('error')}")
            
            # 解析状态信息
            server_status = status_info.get("result", {})
            
            # 获取运行时间
            uptime = server_status.get("uptime", 0)
            
            # 获取连接数
            connections = server_status.get("connections", {}).get("current", 0)
            
            # 获取内存使用情况
            memory_info = server_status.get("mem", {})
            memory_usage = memory_info.get("resident", 0) / 1024  # 转换为MB
            
            # 获取CPU使用率
            cpu_info = server_status.get("cpu", {})
            cpu_usage = cpu_info.get("user", 0) + cpu_info.get("system", 0)
            
            # 构建状态响应
            status_response = {
                "status": self.middleware.status,
                "version": self.middleware.version,
                "uptime": uptime,
                "connections": connections,
                "memory_usage": memory_usage,
                "cpu_usage": cpu_usage,
                "last_checked": timezone.now()
            }
            
            logger.info(f"已获取MongoDB中间件 {self.middleware.id} 状态信息")
            return status_response
            
        except Exception as e:
            logger.error(f"获取MongoDB状态信息失败: {str(e)}")
            return {
                "status": "error",
                "version": self.middleware.version,
                "uptime": 0,
                "connections": 0,
                "memory_usage": 0,
                "cpu_usage": 0,
                "last_checked": timezone.now(),
                "error": str(e)
            }
    
    @retry(max_attempts=3, delay=2, exceptions=(pymongo.errors.PyMongoError, ConnectionError, Exception))
    def backup(self, backup_path: Optional[str] = None) -> Dict[str, Any]:
        """备份MongoDB数据库"""
        logger.info(f"正在备份MongoDB中间件 {self.middleware.id}")
        
        # 如果未指定备份路径，则使用默认路径
        if not backup_path:
            backup_dir = self.config.get('backup_dir', '/tmp/mongodb_backups')
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            backup_path = os.path.join(backup_dir, f"mongodb_{self.middleware.id}_{timestamp}")
            os.makedirs(backup_path, exist_ok=True)
        
        try:
            # 检查是否使用Docker
            if self.config.get('use_docker', False):
                container_name = self.config.get('container_name', f"mongodb-{self.middleware.id}")
                
                # 使用Docker执行备份
                backup_cmd = [
                    "docker", "exec", container_name,
                    "mongodump"
                ]
                
                # 添加认证信息
                if self.config.get('user') and self.config.get('password'):
                    backup_cmd.extend([
                        "--username", self.config.get('user'),
                        "--password", self.config.get('password'),
                        "--authenticationDatabase", self.config.get('auth_source', 'admin')
                    ])
                
                # 添加数据库名称
                if self.config.get('database'):
                    backup_cmd.extend(["--db", self.config.get('database')])
                
                # 添加输出目录
                temp_dir = "/tmp/mongodb_backup"
                backup_cmd.extend(["--out", temp_dir])
                
                # 执行备份命令
                subprocess.run(backup_cmd, check=True)
                
                # 将备份从容器复制到主机
                copy_cmd = ["docker", "cp", f"{container_name}:{temp_dir}/.", backup_path]
                subprocess.run(copy_cmd, check=True)
                
                # 清理容器中的临时备份
                cleanup_cmd = ["docker", "exec", container_name, "rm", "-rf", temp_dir]
                subprocess.run(cleanup_cmd, check=True)
            else:
                # 非Docker方式，直接使用mongodump
                backup_cmd = ["mongodump"]
                
                # 添加连接信息
                backup_cmd.extend([
                    "--host", self.middleware.host,
                    "--port", str(self.middleware.port)
                ])
                
                # 添加认证信息
                if self.config.get('user') and self.config.get('password'):
                    backup_cmd.extend([
                        "--username", self.config.get('user'),
                        "--password", self.config.get('password'),
                        "--authenticationDatabase", self.config.get('auth_source', 'admin')
                    ])
                
                # 添加数据库名称
                if self.config.get('database'):
                    backup_cmd.extend(["--db", self.config.get('database')])
                
                # 添加输出目录
                backup_cmd.extend(["--out", backup_path])
                
                # 执行备份命令
                subprocess.run(backup_cmd, check=True)
            
            # 检查备份是否成功
            if not os.path.exists(backup_path) or not os.listdir(backup_path):
                raise Exception(f"备份目录 {backup_path} 创建失败或为空")
            
            logger.info(f"MongoDB中间件 {self.middleware.id} 已成功备份到 {backup_path}")
            return {"success": True, "backup_path": backup_path}
            
        except Exception as e:
            logger.error(f"备份MongoDB中间件 {self.middleware.id} 失败: {str(e)}")
            if os.path.exists(backup_path):
                try:
                    shutil.rmtree(backup_path)
                except Exception as remove_error:
                    logger.error(f"删除失败的备份目录 {backup_path} 失败: {str(remove_error)}")
            
            raise
    
    @retry(max_attempts=3, delay=2, exceptions=(pymongo.errors.PyMongoError, ConnectionError, Exception))
    def restore(self, backup_path: str) -> Dict[str, Any]:
        """从备份恢复MongoDB数据库"""
        logger.info(f"正在从备份 {backup_path} 恢复MongoDB中间件 {self.middleware.id}")
        
        # 检查备份目录是否存在
        if not os.path.exists(backup_path) or not os.path.isdir(backup_path):
            raise FileNotFoundError(f"备份目录 {backup_path} 不存在或不是目录")
        
        try:
            # 检查是否使用Docker
            if self.config.get('use_docker', False):
                container_name = self.config.get('container_name', f"mongodb-{self.middleware.id}")
                
                # 创建容器内的临时目录
                temp_dir = "/tmp/mongodb_restore"
                mkdir_cmd = ["docker", "exec", container_name, "mkdir", "-p", temp_dir]
                subprocess.run(mkdir_cmd, check=True)
                
                # 将备份复制到容器
                copy_cmd = ["docker", "cp", f"{backup_path}/.", f"{container_name}:{temp_dir}"]
                subprocess.run(copy_cmd, check=True)
                
                # 使用Docker执行恢复
                restore_cmd = [
                    "docker", "exec", container_name,
                    "mongorestore"
                ]
                
                # 添加认证信息
                if self.config.get('user') and self.config.get('password'):
                    restore_cmd.extend([
                        "--username", self.config.get('user'),
                        "--password", self.config.get('password'),
                        "--authenticationDatabase", self.config.get('auth_source', 'admin')
                    ])
                
                # 添加数据库名称
                if self.config.get('database'):
                    restore_cmd.extend(["--db", self.config.get('database')])
                
                # 添加输入目录
                restore_cmd.append(temp_dir)
                
                # 执行恢复命令
                subprocess.run(restore_cmd, check=True)
                
                # 清理容器中的临时目录
                cleanup_cmd = ["docker", "exec", container_name, "rm", "-rf", temp_dir]
                subprocess.run(cleanup_cmd, check=True)
            else:
                # 非Docker方式，直接使用mongorestore
                restore_cmd = ["mongorestore"]
                
                # 添加连接信息
                restore_cmd.extend([
                    "--host", self.middleware.host,
                    "--port", str(self.middleware.port)
                ])
                
                # 添加认证信息
                if self.config.get('user') and self.config.get('password'):
                    restore_cmd.extend([
                        "--username", self.config.get('user'),
                        "--password", self.config.get('password'),
                        "--authenticationDatabase", self.config.get('auth_source', 'admin')
                    ])
                
                # 添加数据库名称
                if self.config.get('database'):
                    restore_cmd.extend(["--db", self.config.get('database')])
                
                # 添加输入目录
                restore_cmd.append(backup_path)
                
                # 执行恢复命令
                subprocess.run(restore_cmd, check=True)
            
            # 更新中间件状态
            self.middleware.status = 'running'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            logger.info(f"MongoDB中间件 {self.middleware.id} 已成功从备份 {backup_path} 恢复")
            return {"success": True}
            
        except Exception as e:
            logger.error(f"恢复MongoDB中间件 {self.middleware.id} 失败: {str(e)}")
            
            # 更新中间件状态为错误
            self.middleware.status = 'error'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            raise
    
    @retry(max_attempts=3, delay=2, exceptions=(pymongo.errors.PyMongoError, ConnectionError, Exception))
    def validate_config(self, config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """验证MongoDB配置有效性"""
        logger.info(f"正在验证MongoDB中间件 {self.middleware.id} 的配置")
        
        # 验证连接参数
        try:
            # 创建临时连接测试配置有效性
            connection_params = {
                'host': self.middleware.host,
                'port': self.middleware.port,
                'maxPoolSize': config.get('max_pool_size', 5),
                'serverSelectionTimeoutMS': 5000,  # 5秒超时
                'connectTimeoutMS': 5000
            }
            
            # 添加认证信息
            if config.get('user') and config.get('password'):
                connection_params['username'] = config.get('user')
                connection_params['password'] = config.get('password')
                connection_params['authSource'] = config.get('auth_source', 'admin')
            
            # 测试连接
            client = pymongo.MongoClient(**connection_params)
            client.admin.command('ping')
            client.close()
            
            logger.info(f"MongoDB中间件 {self.middleware.id} 配置验证成功")
            return True, None
        except Exception as e:
            logger.error(f"MongoDB配置验证失败: {str(e)}")
            return False, f"配置验证失败: {str(e)}"
    
    @retry(max_attempts=3, delay=2, exceptions=(pymongo.errors.PyMongoError, ConnectionError, Exception))
    def update_config(self, new_config: Dict[str, Any]) -> Dict[str, Any]:
        """更新MongoDB配置"""
        logger.info(f"正在更新MongoDB中间件 {self.middleware.id} 的配置")
        
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
                container_name = self.config.get('container_name', f"mongodb-{self.middleware.id}")
                
                # 对于Docker容器，某些配置需要重新创建容器才能生效
                # 这里仅处理可以热更新的配置
                
                # 例如，更新最大连接数
                if 'max_connections' in new_config:
                    max_connections = new_config.get('max_connections')
                    set_cmd = [
                        "docker", "exec", container_name,
                        "mongo", "--eval",
                        f"db.adminCommand({{setParameter: 1, maxConnections: {max_connections}}})"
                    ]
                    
                    # 添加认证信息
                    if self.config.get('user') and self.config.get('password'):
                        set_cmd[3:3] = [
                            "-u", self.config.get('user'),
                            "-p", self.config.get('password'),
                            "--authenticationDatabase", self.config.get('auth_source', 'admin')
                        ]
                    
                    subprocess.run(set_cmd, check=True)
            else:
                # 非Docker方式，直接使用mongo命令更新配置
                # 这里仅处理可以热更新的配置
                
                # 例如，更新最大连接数
                if 'max_connections' in new_config:
                    max_connections = new_config.get('max_connections')
                    set_cmd = [
                        "mongo",
                        "--host", self.middleware.host,
                        "--port", str(self.middleware.port),
                        "--eval", f"db.adminCommand({{setParameter: 1, maxConnections: {max_connections}}})"
                    ]
                    
                    # 添加认证信息
                    if self.config.get('user') and self.config.get('password'):
                        set_cmd[5:5] = [
                            "-u", self.config.get('user'),
                            "-p", self.config.get('password'),
                            "--authenticationDatabase", self.config.get('auth_source', 'admin')
                        ]
                    
                    subprocess.run(set_cmd, check=True)
            
            # 更新中间件配置记录
            self.middleware.config.config_data.update(new_config)
            self.middleware.config.updated_at = timezone.now()
            self.middleware.config.save()
            
            # 更新中间件最后更新时间
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            logger.info(f"MongoDB中间件 {self.middleware.id} 配置已更新")
            return {"success": True, "config_updated": True}
            
        except Exception as e:
            logger.error(f"更新MongoDB中间件 {self.middleware.id} 配置失败: {str(e)}")
            
            # 恢复旧配置
            self.config = old_config
            
            # 更新中间件状态为错误
            self.middleware.status = 'error'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            raise