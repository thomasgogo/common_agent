import logging
import time
import subprocess
import pymysql
from typing import Dict, Any, Optional, Tuple
from django.utils import timezone
from django.conf import settings
import os
import shutil
from datetime import datetime

# 导入基础适配器类
from .adapters import MiddlewareAdapter, retry

# 配置日志
logger = logging.getLogger(__name__)


class MySQLAdapter(MiddlewareAdapter):
    """MySQL中间件适配器"""
    
    @retry(max_attempts=3, delay=2, exceptions=(pymysql.Error, ConnectionError))
    def _get_connection(self):
        """获取MySQL数据库连接"""
        return pymysql.connect(
            host=self.middleware.host,
            port=self.middleware.port,
            user=self.config.get('user'),
            password=self.config.get('password'),
            database=self.config.get('database', ''),
            charset=self.config.get('charset', 'utf8mb4'),
            connect_timeout=self.config.get('connection_timeout', 10),
            cursorclass=pymysql.cursors.DictCursor
        )
    
    def _execute_query(self, query: str, params=None) -> Dict[str, Any]:
        """执行MySQL查询"""
        try:
            connection = self._get_connection()
            with connection.cursor() as cursor:
                cursor.execute(query, params or ())
                if query.strip().upper().startswith('SELECT'):
                    result = cursor.fetchall()
                else:
                    connection.commit()
                    result = {"affected_rows": cursor.rowcount}
            connection.close()
            return {"success": True, "result": result}
        except Exception as e:
            logger.error(f"MySQL查询执行失败: {str(e)}")
            return {"success": False, "error": str(e)}
    
    @retry(max_attempts=3, delay=2, exceptions=(pymysql.Error, ConnectionError, Exception))
    def start(self) -> Dict[str, Any]:
        """启动MySQL服务"""
        logger.info(f"正在启动MySQL中间件: {self.middleware.id}")
        
        # 检查是否使用Docker
        if self.config.get('use_docker', False):
            container_name = self.config.get('container_name', f"mysql-{self.middleware.id}")
            image = self.config.get('docker_image', 'mysql:latest')
            
            # 检查容器是否存在
            check_cmd = ["docker", "ps", "-a", "-q", "-f", f"name={container_name}"]
            result = subprocess.run(check_cmd, capture_output=True, text=True)
            
            if result.stdout.strip():
                # 容器存在，启动它
                start_cmd = ["docker", "start", container_name]
                subprocess.run(start_cmd, check=True)
            else:
                # 容器不存在，创建并启动
                port_mapping = f"{self.middleware.port}:3306"
                run_cmd = [
                    "docker", "run", "-d",
                    "--name", container_name,
                    "-p", port_mapping,
                    "-e", f"MYSQL_ROOT_PASSWORD={self.config.get('password')}"
                ]
                
                # 添加数据库名称
                if self.config.get('database'):
                    run_cmd.extend(["-e", f"MYSQL_DATABASE={self.config.get('database')}"]) 
                
                # 添加持久化目录映射
                if self.config.get('data_dir'):
                    volume = f"{self.config.get('data_dir')}:/var/lib/mysql"
                    run_cmd.extend(["-v", volume])
                
                # 添加自定义配置文件
                if self.config.get('config_file'):
                    config_volume = f"{self.config.get('config_file')}:/etc/mysql/conf.d/custom.cnf"
                    run_cmd.extend(["-v", config_volume])
                
                run_cmd.append(image)
                subprocess.run(run_cmd, check=True)
        else:
            # 非Docker方式，使用系统服务
            # 这里假设使用systemd管理MySQL服务
            service_name = self.config.get('service_name', 'mysql')
            subprocess.run(["systemctl", "start", service_name], check=True)
        
        # 等待服务启动
        max_retries = 10
        retry_interval = 3
        for i in range(max_retries):
            try:
                # 尝试连接数据库
                connection = self._get_connection()
                connection.close()
                break
            except Exception as e:
                if i == max_retries - 1:
                    logger.error(f"MySQL服务启动失败: {str(e)}")
                    raise
                logger.warning(f"等待MySQL服务启动 ({i+1}/{max_retries}): {str(e)}")
                time.sleep(retry_interval)
        
        # 验证服务是否成功启动
        status_info = self._execute_query("SELECT VERSION() as version")
        if not status_info.get("success"):
            raise Exception(f"无法获取MySQL版本信息: {status_info.get('error')}")
        
        # 更新中间件状态
        self.middleware.status = 'running'
        self.middleware.last_updated = timezone.now()
        self.middleware.save()
        
        logger.info(f"MySQL中间件 {self.middleware.id} 已成功启动")
        return {"success": True, "info": status_info.get("result")}
    
    @retry(max_attempts=3, delay=2, exceptions=(pymysql.Error, ConnectionError, Exception))
    def stop(self) -> Dict[str, Any]:
        """停止MySQL服务"""
        logger.info(f"正在停止MySQL中间件: {self.middleware.id}")
        
        # 检查是否使用Docker
        if self.config.get('use_docker', False):
            container_name = self.config.get('container_name', f"mysql-{self.middleware.id}")
            subprocess.run(["docker", "stop", container_name], check=True)
        else:
            # 非Docker方式，使用系统服务
            service_name = self.config.get('service_name', 'mysql')
            subprocess.run(["systemctl", "stop", service_name], check=True)
        
        # 更新中间件状态
        self.middleware.status = 'stopped'
        self.middleware.last_updated = timezone.now()
        self.middleware.save()
        
        logger.info(f"MySQL中间件 {self.middleware.id} 已成功停止")
        return {"success": True}
    
    @retry(max_attempts=3, delay=2, exceptions=(pymysql.Error, ConnectionError, Exception))
    def upgrade(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """升级MySQL服务"""
        target_version = params.get("target_version")
        backup = params.get("backup", True)
        force = params.get("force", False)
        
        logger.info(f"正在升级MySQL中间件 {self.middleware.id} 到版本 {target_version}")
        
        # 更新中间件状态为更新中
        self.middleware.status = 'updating'
        self.middleware.last_updated = timezone.now()
        self.middleware.save()
        
        # 如果需要备份，先进行备份
        backup_path = None
        if backup:
            backup_result = self.backup()
            backup_path = backup_result.get('backup_path')
            logger.info(f"已备份MySQL中间件 {self.middleware.id} 到 {backup_path}")
        
        try:
            # 检查是否使用Docker
            if self.config.get('use_docker', False):
                container_name = self.config.get('container_name', f"mysql-{self.middleware.id}")
                image = f"mysql:{target_version}"
                
                # 停止并删除旧容器
                subprocess.run(["docker", "stop", container_name], check=True)
                subprocess.run(["docker", "rm", container_name], check=True)
                
                # 拉取新版本镜像
                subprocess.run(["docker", "pull", image], check=True)
                
                # 创建并启动新容器
                port_mapping = f"{self.middleware.port}:3306"
                run_cmd = [
                    "docker", "run", "-d",
                    "--name", container_name,
                    "-p", port_mapping,
                    "-e", f"MYSQL_ROOT_PASSWORD={self.config.get('password')}"
                ]
                
                # 添加数据库名称
                if self.config.get('database'):
                    run_cmd.extend(["-e", f"MYSQL_DATABASE={self.config.get('database')}"]) 
                
                # 添加持久化目录映射
                if self.config.get('data_dir'):
                    volume = f"{self.config.get('data_dir')}:/var/lib/mysql"
                    run_cmd.extend(["-v", volume])
                
                # 添加自定义配置文件
                if self.config.get('config_file'):
                    config_volume = f"{self.config.get('config_file')}:/etc/mysql/conf.d/custom.cnf"
                    run_cmd.extend(["-v", config_volume])
                
                run_cmd.append(image)
                subprocess.run(run_cmd, check=True)
            else:
                # 非Docker方式，使用系统包管理器升级
                # 这里假设使用apt作为包管理器
                subprocess.run(["apt-get", "update"], check=True)
                subprocess.run(["apt-get", "install", "-y", f"mysql-server={target_version}*"], check=True)
                
                # 重启服务
                service_name = self.config.get('service_name', 'mysql')
                subprocess.run(["systemctl", "restart", service_name], check=True)
            
            # 等待服务启动
            max_retries = 10
            retry_interval = 3
            for i in range(max_retries):
                try:
                    # 尝试连接数据库
                    connection = self._get_connection()
                    connection.close()
                    break
                except Exception as e:
                    if i == max_retries - 1:
                        logger.error(f"MySQL服务启动失败: {str(e)}")
                        raise
                    logger.warning(f"等待MySQL服务启动 ({i+1}/{max_retries}): {str(e)}")
                    time.sleep(retry_interval)
            
            # 验证服务是否成功启动
            status_info = self._execute_query("SELECT VERSION() as version")
            if not status_info.get("success"):
                raise Exception(f"无法获取MySQL版本信息: {status_info.get('error')}")
            
            # 更新中间件版本和状态
            self.middleware.version = target_version
            self.middleware.status = 'running'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            logger.info(f"MySQL中间件 {self.middleware.id} 已成功升级到版本 {target_version}")
            return {"success": True, "version": target_version}
            
        except Exception as e:
            logger.error(f"升级MySQL中间件 {self.middleware.id} 失败: {str(e)}")
            
            # 如果有备份且升级失败，尝试恢复
            if backup_path:
                try:
                    logger.info(f"尝试从备份 {backup_path} 恢复MySQL中间件 {self.middleware.id}")
                    self.restore(backup_path)
                except Exception as restore_error:
                    logger.error(f"恢复MySQL中间件 {self.middleware.id} 失败: {str(restore_error)}")
            
            # 更新中间件状态为错误
            self.middleware.status = 'error'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            raise
    
    @retry(max_attempts=3, delay=2, exceptions=(pymysql.Error, ConnectionError, Exception))
    def get_status(self) -> Dict[str, Any]:
        """获取MySQL状态信息"""
        logger.info(f"获取MySQL中间件 {self.middleware.id} 状态信息")
        
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
            
            # 获取MySQL状态信息
            status_info = self._execute_query("SHOW STATUS")
            if not status_info.get("success"):
                raise Exception(f"无法获取MySQL状态信息: {status_info.get('error')}")
            
            # 解析状态信息
            status_dict = {}
            for item in status_info.get("result", []):
                if isinstance(item, dict) and 'Variable_name' in item and 'Value' in item:
                    status_dict[item['Variable_name']] = item['Value']
            
            # 获取运行时间
            uptime_info = self._execute_query("SHOW GLOBAL STATUS LIKE 'Uptime'")
            uptime = 0
            if uptime_info.get("success") and uptime_info.get("result"):
                for item in uptime_info.get("result", []):
                    if item.get('Value'):
                        try:
                            uptime = int(item.get('Value', 0))
                        except (ValueError, TypeError):
                            uptime = 0
            
            # 获取连接数
            connections_info = self._execute_query("SHOW GLOBAL STATUS LIKE 'Threads_connected'")
            connections = 0
            if connections_info.get("success") and connections_info.get("result"):
                for item in connections_info.get("result", []):
                    if item.get('Value'):
                        try:
                            connections = int(item.get('Value', 0))
                        except (ValueError, TypeError):
                            connections = 0
            
            # 获取内存使用情况
            memory_info = self._execute_query("SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_bytes_data'")
            memory_usage = 0
            if memory_info.get("success") and memory_info.get("result"):
                for item in memory_info.get("result", []):
                    if item.get('Value'):
                        try:
                            # 转换为MB
                            memory_usage = float(item.get('Value', 0)) / (1024 * 1024)
                        except (ValueError, TypeError):
                            memory_usage = 0
            
            # 在实际应用中，获取CPU使用率需要系统级别的监控
            # 这里简单模拟一个值
            cpu_usage = 2.5
            
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
            
            logger.info(f"已获取MySQL中间件 {self.middleware.id} 状态信息")
            return status_response
            
        except Exception as e:
            logger.error(f"获取MySQL状态信息失败: {str(e)}")
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
    
    @retry(max_attempts=3, delay=2, exceptions=(pymysql.Error, ConnectionError, Exception))
    def backup(self, backup_path: Optional[str] = None) -> Dict[str, Any]:
        """备份MySQL数据库"""
        logger.info(f"正在备份MySQL中间件 {self.middleware.id}")
        
        # 如果未指定备份路径，则使用默认路径
        if not backup_path:
            backup_dir = self.config.get('backup_dir', '/tmp/mysql_backups')
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            backup_path = os.path.join(backup_dir, f"mysql_{self.middleware.id}_{timestamp}.sql")
        
        try:
            # 检查是否使用Docker
            if self.config.get('use_docker', False):
                container_name = self.config.get('container_name', f"mysql-{self.middleware.id}")
                
                # 使用Docker执行备份
                backup_cmd = [
                    "docker", "exec", container_name,
                    "mysqldump",
                    "-u", self.config.get('user'),
                    f"--password={self.config.get('password')}",
                    "--all-databases",
                    "--single-transaction",
                    "--quick",
                    "--lock-tables=false"
                ]
                
                # 将输出重定向到备份文件
                with open(backup_path, 'w') as backup_file:
                    subprocess.run(backup_cmd, stdout=backup_file, check=True)
            else:
                # 非Docker方式，直接使用mysqldump
                backup_cmd = [
                    "mysqldump",
                    "-h", self.middleware.host,
                    "-P", str(self.middleware.port),
                    "-u", self.config.get('user'),
                    f"--password={self.config.get('password')}",
                    "--all-databases",
                    "--single-transaction",
                    "--quick",
                    "--lock-tables=false",
                    "-r", backup_path
                ]
                
                subprocess.run(backup_cmd, check=True)
            
            # 检查备份文件是否创建成功
            if not os.path.exists(backup_path) or os.path.getsize(backup_path) == 0:
                raise Exception(f"备份文件 {backup_path} 创建失败或为空")
            
            logger.info(f"MySQL中间件 {self.middleware.id} 已成功备份到 {backup_path}")
            return {"success": True, "backup_path": backup_path}
            
        except Exception as e:
            logger.error(f"备份MySQL中间件 {self.middleware.id} 失败: {str(e)}")
            if os.path.exists(backup_path):
                try:
                    os.remove(backup_path)
                except Exception as remove_error:
                    logger.error(f"删除失败的备份文件 {backup_path} 失败: {str(remove_error)}")
            
            raise
    
    @retry(max_attempts=3, delay=2, exceptions=(pymysql.Error, ConnectionError, Exception))
    def restore(self, backup_path: str) -> Dict[str, Any]:
        """从备份恢复MySQL数据库"""
        logger.info(f"正在从备份 {backup_path} 恢复MySQL中间件 {self.middleware.id}")
        
        # 检查备份文件是否存在
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"备份文件 {backup_path} 不存在")
        
        try:
            # 检查是否使用Docker
            if self.config.get('use_docker', False):
                container_name = self.config.get('container_name', f"mysql-{self.middleware.id}")
                
                # 将备份文件复制到容器内
                temp_path_in_container = "/tmp/mysql_backup.sql"
                copy_cmd = ["docker", "cp", backup_path, f"{container_name}:{temp_path_in_container}"]
                subprocess.run(copy_cmd, check=True)
                
                # 使用Docker执行恢复
                restore_cmd = [
                    "docker", "exec", container_name,
                    "mysql",
                    "-u", self.config.get('user'),
                    f"--password={self.config.get('password')}",
                    "-e", f"source {temp_path_in_container}"
                ]
                
                subprocess.run(restore_cmd, check=True)
                
                # 清理临时文件
                cleanup_cmd = ["docker", "exec", container_name, "rm", temp_path_in_container]
                subprocess.run(cleanup_cmd, check=True)
            else:
                # 非Docker方式，直接使用mysql命令
                restore_cmd = [
                    "mysql",
                    "-h", self.middleware.host,
                    "-P", str(self.middleware.port),
                    "-u", self.config.get('user'),
                    f"--password={self.config.get('password')}",
                    "-e", f"source {backup_path}"
                ]
                
                subprocess.run(restore_cmd, check=True)
            
            # 更新中间件状态
            self.middleware.status = 'running'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            logger.info(f"MySQL中间件 {self.middleware.id} 已成功从备份 {backup_path} 恢复")
            return {"success": True}
            
        except Exception as e:
            logger.error(f"恢复MySQL中间件 {self.middleware.id} 失败: {str(e)}")
            
            # 更新中间件状态为错误
            self.middleware.status = 'error'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            raise
    
    @retry(max_attempts=3, delay=2, exceptions=(pymysql.Error, ConnectionError, Exception))
    def validate_config(self, config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """验证MySQL配置有效性"""
        logger.info(f"正在验证MySQL中间件 {self.middleware.id} 的配置")
        
        # 检查必要的配置项
        required_fields = ['user', 'password']
        for field in required_fields:
            if field not in config:
                return False, f"缺少必要的配置项: {field}"
        
        # 验证连接参数
        try:
            # 创建临时连接测试配置有效性
            test_connection = pymysql.connect(
                host=self.middleware.host,
                port=self.middleware.port,
                user=config.get('user'),
                password=config.get('password'),
                database=config.get('database', ''),
                charset=config.get('charset', 'utf8mb4'),
                connect_timeout=config.get('connection_timeout', 10),
                cursorclass=pymysql.cursors.DictCursor
            )
            test_connection.close()
            
            logger.info(f"MySQL中间件 {self.middleware.id} 配置验证成功")
            return True, None
        except Exception as e:
            logger.error(f"MySQL配置验证失败: {str(e)}")
            return False, f"配置验证失败: {str(e)}"
    
    @retry(max_attempts=3, delay=2, exceptions=(pymysql.Error, ConnectionError, Exception))
    def update_config(self, new_config: Dict[str, Any]) -> Dict[str, Any]:
        """更新MySQL配置"""
        logger.info(f"正在更新MySQL中间件 {self.middleware.id} 的配置")
        
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
                container_name = self.config.get('container_name', f"mysql-{self.middleware.id}")
                
                # 对于Docker容器，某些配置需要重新创建容器才能生效
                # 这里仅处理可以热更新的配置
                
                # 例如，更新最大连接数
                if 'max_connections' in new_config:
                    max_connections = new_config.get('max_connections')
                    set_cmd = [
                        "docker", "exec", container_name,
                        "mysql",
                        "-u", self.config.get('user'),
                        f"--password={self.config.get('password')}",
                        "-e", f"SET GLOBAL max_connections = {max_connections};"
                    ]
                    subprocess.run(set_cmd, check=True)
                
                # 更新其他可热更新的参数
                if 'wait_timeout' in new_config:
                    wait_timeout = new_config.get('wait_timeout')
                    set_cmd = [
                        "docker", "exec", container_name,
                        "mysql",
                        "-u", self.config.get('user'),
                        f"--password={self.config.get('password')}",
                        "-e", f"SET GLOBAL wait_timeout = {wait_timeout};"
                    ]
                    subprocess.run(set_cmd, check=True)
            else:
                # 非Docker方式，直接使用mysql命令更新配置
                # 这里仅处理可以热更新的配置
                
                # 例如，更新最大连接数
                if 'max_connections' in new_config:
                    max_connections = new_config.get('max_connections')
                    set_cmd = [
                        "mysql",
                        "-h", self.middleware.host,
                        "-P", str(self.middleware.port),
                        "-u", self.config.get('user'),
                        f"--password={self.config.get('password')}",
                        "-e", f"SET GLOBAL max_connections = {max_connections};"
                    ]
                    subprocess.run(set_cmd, check=True)
                
                # 更新其他可热更新的参数
                if 'wait_timeout' in new_config:
                    wait_timeout = new_config.get('wait_timeout')
                    set_cmd = [
                        "mysql",
                        "-h", self.middleware.host,
                        "-P", str(self.middleware.port),
                        "-u", self.config.get('user'),
                        f"--password={self.config.get('password')}",
                        "-e", f"SET GLOBAL wait_timeout = {wait_timeout};"
                    ]
                    subprocess.run(set_cmd, check=True)
            
            # 更新中间件配置记录
            self.middleware.config.config_data.update(new_config)
            self.middleware.config.updated_at = timezone.now()
            self.middleware.config.save()
            
            # 更新中间件最后更新时间
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            logger.info(f"MySQL中间件 {self.middleware.id} 配置已更新")
            return {"success": True, "config_updated": True}
            
        except Exception as e:
            logger.error(f"更新MySQL中间件 {self.middleware.id} 配置失败: {str(e)}")
            
            # 恢复旧配置
            self.config = old_config
            
            # 更新中间件状态为错误
            self.middleware.status = 'error'
            self.middleware.last_updated = timezone.now()
            self.middleware.save()
            
            raise Exception(f"更新MySQL中间件配置失败: {str(e)}")