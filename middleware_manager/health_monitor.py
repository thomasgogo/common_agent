import logging
import time
import threading
import json
import os
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
from django.conf import settings
from django.utils import timezone

# 导入错误处理模块
from .error_handler import OperationResult

# 配置日志
logger = logging.getLogger(__name__)  

class HealthStatus:
    """健康状态枚举"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"

class HealthCheck:
    """健康检查基类"""
    
    def __init__(self, name: str, description: str, check_interval: int = 60):
        """
        初始化健康检查
        
        Args:
            name: 检查名称
            description: 检查描述
            check_interval: 检查间隔（秒）
        """
        self.name = name
        self.description = description
        self.check_interval = check_interval
        self.last_check_time = None
        self.last_status = HealthStatus.UNKNOWN
        self.last_message = "未执行检查"
        self.history = []
    
    def check(self) -> Dict[str, Any]:
        """执行健康检查"""
        raise NotImplementedError("子类必须实现此方法")
    
    def get_status(self) -> Dict[str, Any]:
        """获取当前状态"""
        return {
            "name": self.name,
            "description": self.description,
            "last_check_time": self.last_check_time.isoformat() if self.last_check_time else None,
            "status": self.last_status,
            "message": self.last_message
        }
    
    def get_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取历史记录"""
        return self.history[-limit:] if limit > 0 else self.history

class MiddlewareHealthCheck(HealthCheck):
    """中间件健康检查"""
    
    def __init__(self, middleware, adapter, **kwargs):
        """
        初始化中间件健康检查
        
        Args:
            middleware: 中间件对象
            adapter: 中间件适配器
        """
        name = f"{middleware.type}-{middleware.id}"
        description = f"检查{middleware.name}的健康状态"
        super().__init__(name, description, **kwargs)
        self.middleware = middleware
        self.adapter = adapter
        self.thresholds = {
            "memory_usage_warning": 70,  # 内存使用率警告阈值（%）
            "memory_usage_critical": 90,  # 内存使用率严重阈值（%）
            "cpu_usage_warning": 70,     # CPU使用率警告阈值（%）
            "cpu_usage_critical": 90,    # CPU使用率严重阈值（%）
            "connection_usage_warning": 70,  # 连接使用率警告阈值（%）
            "connection_usage_critical": 90,  # 连接使用率严重阈值（%）
            "response_time_warning": 1.0,  # 响应时间警告阈值（秒）
            "response_time_critical": 3.0   # 响应时间严重阈值（秒）
        }
    
    def check(self) -> Dict[str, Any]:
        """执行中间件健康检查"""
        start_time = time.time()
        try:
            # 获取中间件状态
            status_result = self.adapter.get_status()
            end_time = time.time()
            response_time = end_time - start_time
            
            if not status_result.get("success", False):
                status = HealthStatus.CRITICAL
                message = f"无法获取中间件状态: {status_result.get('error', '未知错误')}"
            else:
                status_info = status_result.get("status", {})
                
                # 检查中间件状态
                if status_info.get("status") != "running":
                    status = HealthStatus.CRITICAL
                    message = f"中间件未运行，当前状态: {status_info.get('status', '未知')}"
                else:
                    # 检查各项指标
                    issues = []
                    
                    # 检查内存使用
                    if "used_memory_human" in status_info:
                        memory_usage = self._parse_memory_usage(status_info.get("used_memory_human", "0"))
                        memory_limit = self._parse_memory_usage(status_info.get("maxmemory_human", "0"))
                        
                        if memory_limit > 0:
                            memory_usage_percent = (memory_usage / memory_limit) * 100
                            if memory_usage_percent >= self.thresholds["memory_usage_critical"]:
                                issues.append(f"内存使用率达到严重水平: {memory_usage_percent:.1f}%")
                            elif memory_usage_percent >= self.thresholds["memory_usage_warning"]:
                                issues.append(f"内存使用率达到警告水平: {memory_usage_percent:.1f}%")
                    
                    # 检查CPU使用
                    if "cpu_usage" in status_info:
                        cpu_usage = float(status_info.get("cpu_usage", 0))
                        if cpu_usage >= self.thresholds["cpu_usage_critical"]:
                            issues.append(f"CPU使用率达到严重水平: {cpu_usage:.1f}%")
                        elif cpu_usage >= self.thresholds["cpu_usage_warning"]:
                            issues.append(f"CPU使用率达到警告水平: {cpu_usage:.1f}%")
                    
                    # 检查连接数
                    if "connected_clients" in status_info and "maxclients" in status_info:
                        connected = int(status_info.get("connected_clients", 0))
                        max_clients = int(status_info.get("maxclients", 0))
                        
                        if max_clients > 0:
                            connection_usage_percent = (connected / max_clients) * 100
                            if connection_usage_percent >= self.thresholds["connection_usage_critical"]:
                                issues.append(f"连接使用率达到严重水平: {connection_usage_percent:.1f}%")
                            elif connection_usage_percent >= self.thresholds["connection_usage_warning"]:
                                issues.append(f"连接使用率达到警告水平: {connection_usage_percent:.1f}%")
                    
                    # 检查响应时间
                    if response_time >= self.thresholds["response_time_critical"]:
                        issues.append(f"响应时间达到严重水平: {response_time:.2f}秒")
                    elif response_time >= self.thresholds["response_time_warning"]:
                        issues.append(f"响应时间达到警告水平: {response_time:.2f}秒")
                    
                    # 根据问题确定状态
                    if not issues:
                        status = HealthStatus.HEALTHY
                        message = "中间件运行正常"
                    else:
                        # 检查是否有严重问题
                        has_critical = any("严重" in issue for issue in issues)
                        status = HealthStatus.CRITICAL if has_critical else HealthStatus.WARNING
                        message = "\n".join(issues)
            
            # 更新状态
            self.last_check_time = timezone.now()
            self.last_status = status
            self.last_message = message
            
            # 记录历史
            history_entry = {
                "timestamp": self.last_check_time.isoformat(),
                "status": status,
                "message": message,
                "response_time": response_time
            }
            self.history.append(history_entry)
            
            # 如果历史记录过多，删除旧记录
            max_history = 100
            if len(self.history) > max_history:
                self.history = self.history[-max_history:]
            
            return {
                "success": True,
                "status": status,
                "message": message,
                "response_time": response_time,
                "details": status_info
            }
            
        except Exception as e:
            logger.error(f"执行健康检查失败: {str(e)}")
            end_time = time.time()
            response_time = end_time - start_time
            
            # 更新状态
            self.last_check_time = timezone.now()
            self.last_status = HealthStatus.CRITICAL
            self.last_message = f"健康检查异常: {str(e)}"
            
            # 记录历史
            history_entry = {
                "timestamp": self.last_check_time.isoformat(),
                "status": HealthStatus.CRITICAL,
                "message": self.last_message,
                "response_time": response_time
            }
            self.history.append(history_entry)
            
            return {
                "success": False,
                "status": HealthStatus.CRITICAL,
                "message": f"健康检查异常: {str(e)}",
                "response_time": response_time
            }
    
    def _parse_memory_usage(self, memory_str: str) -> float:
        """解析内存使用字符串（如 '100MB'）为字节数"""
        try:
            if not memory_str or memory_str == "0":
                return 0
                
            # 移除单位并转换为浮点数
            value = float(''.join(c for c in memory_str if c.isdigit() or c == '.'))
            
            # 根据单位转换为字节
            if 'K' in memory_str or 'k' in memory_str:
                return value * 1024
            elif 'M' in memory_str or 'm' in memory_str:
                return value * 1024 * 1024
            elif 'G' in memory_str or 'g' in memory_str:
                return value * 1024 * 1024 * 1024
            else:
                return value
        except Exception:
            return 0

class HealthMonitor:
    """健康监控系统"""
    
    def __init__(self):
        self.checks = {}
        self.alerters = []
        self.running = False
        self.monitor_thread = None
        self.check_lock = threading.Lock()
    
    def add_check(self, check: HealthCheck) -> None:
        """添加健康检查"""
        self.checks[check.name] = check
    
    def remove_check(self, check_name: str) -> None:
        """移除健康检查"""
        if check_name in self.checks:
            del self.checks[check_name]
    
    def add_alerter(self, alerter) -> None:
        """添加告警器"""
        self.alerters.append(alerter)
    
    def start(self) -> None:
        """启动监控"""
        if self.running:
            return
            
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("健康监控系统已启动")
    
    def stop(self) -> None:
        """停止监控"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
            self.monitor_thread = None
        logger.info("健康监控系统已停止")
    
    def _monitor_loop(self) -> None:
        """监控循环"""
        while self.running:
            # 检查所有健康检查
            for check_name, check in list(self.checks.items()):
                # 检查是否需要执行检查
                if not check.last_check_time or \
                   (timezone.now() - check.last_check_time).total_seconds() >= check.check_interval:
                    with self.check_lock:
                        try:
                            # 执行健康检查
                            result = check.check()
                            
                            # 如果状态为警告或严重，触发告警
                            if result.get("status") in [HealthStatus.WARNING, HealthStatus.CRITICAL]:
                                self._trigger_alert(check, result)
                                
                        except Exception as e:
                            logger.error(f"执行健康检查 {check_name} 失败: {str(e)}")
            
            # 休眠一段时间
            time.sleep(1)
    
    def _trigger_alert(self, check: HealthCheck, result: Dict[str, Any]) -> None:
        """触发告警"""
        for alerter in self.alerters:
            try:
                alerter.alert(check, result)
            except Exception as e:
                logger.error(f"触发告警失败: {str(e)}")

class AlertLevel:
    """告警级别枚举"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

class AlertBase:
    """告警器基类"""
    
    def __init__(self, name: str, min_level: str = AlertLevel.WARNING):
        """
        初始化告警器
        
        Args:
            name: 告警器名称
            min_level: 最小告警级别
        """
        self.name = name
        self.min_level = min_level
        self.last_alert_time = {}
        self.cooldown_period = 300  # 默认冷却时间5分钟
    
    def should_alert(self, check: HealthCheck, result: Dict[str, Any]) -> bool:
        """
        判断是否应该发送告警
        
        Args:
            check: 健康检查对象
            result: 检查结果
            
        Returns:
            是否应该发送告警
        """
        # 获取告警级别
        status = result.get("status")
        level = AlertLevel.INFO
        if status == HealthStatus.WARNING:
            level = AlertLevel.WARNING
        elif status == HealthStatus.CRITICAL:
            level = AlertLevel.CRITICAL
        
        # 检查是否达到最小告警级别
        if level == AlertLevel.INFO and self.min_level != AlertLevel.INFO:
            return False
        if level == AlertLevel.WARNING and self.min_level == AlertLevel.CRITICAL:
            return False
        
        # 检查冷却时间
        check_name = check.name
        now = timezone.now()
        if check_name in self.last_alert_time:
            last_time = self.last_alert_time[check_name]
            if (now - last_time).total_seconds() < self.cooldown_period:
                return False
        
        # 更新最后告警时间
        self.last_alert_time[check_name] = now
        return True
    
    def alert(self, check: HealthCheck, result: Dict[str, Any]) -> None:
        """发送告警"""
        if not self.should_alert(check, result):
            return
        
        self._send_alert(check, result)
    
    def _send_alert(self, check: HealthCheck, result: Dict[str, Any]) -> None:
        """发送告警的具体实现"""
        raise NotImplementedError("子类必须实现此方法")

class EmailAlerter(AlertBase):
    """邮件告警器"""
    
    def __init__(self, smtp_server: str, smtp_port: int, sender: str, password: str, recipients: List[str], **kwargs):
        """
        初始化邮件告警器
        
        Args:
            smtp_server: SMTP服务器地址
            smtp_port: SMTP服务器端口
            sender: 发件人邮箱
            password: 发件人密码
            recipients: 收件人邮箱列表
        """
        super().__init__("email_alerter", **kwargs)
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender = sender
        self.password = password
        self.recipients = recipients
    
    def _send_alert(self, check: HealthCheck, result: Dict[str, Any]) -> None:
        """发送邮件告警"""
        status = result.get("status")
        message = result.get("message")
        
        # 构建邮件主题
        if status == HealthStatus.WARNING:
            subject = f"[警告] {check.name} 健康检查告警"
        elif status == HealthStatus.CRITICAL:
            subject = f"[严重] {check.name} 健康检查告警"
        else:
            subject = f"[信息] {check.name} 健康检查通知"
        
        # 构建邮件内容
        body = f"""<html>
<body>
<h2>{subject}</h2>
<p><strong>中间件:</strong> {check.middleware.name}</p>
<p><strong>类型:</strong> {check.middleware.type}</p>
<p><strong>状态:</strong> {status}</p>
<p><strong>时间:</strong> {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
<p><strong>消息:</strong> {message}</p>
<p><strong>详情:</strong></p>
<pre>{json.dumps(result.get('details', {}), indent=2, ensure_ascii=False)}</pre>
</body>
</html>
"""
        
        # 创建邮件
        msg = MIMEMultipart()
        msg['From'] = self.sender
        msg['To'] = ", ".join(self.recipients)
        msg['Subject'] = subject
        
        # 添加HTML内容
        msg.attach(MIMEText(body, 'html'))
        
        # 发送邮件
        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender, self.password)
            server.send_message(msg)
            server.quit()
            logger.info(f"已发送邮件告警: {subject}")
        except Exception as e:
            logger.error(f"发送邮件告警失败: {str(e)}")

class WebhookAlerter(AlertBase):
    """Webhook告警器"""
    
    def __init__(self, webhook_url: str, headers: Optional[Dict[str, str]] = None, **kwargs):
        """
        初始化Webhook告警器
        
        Args:
            webhook_url: Webhook URL
            headers: 请求头
        """
        super().__init__("webhook_alerter", **kwargs)
        self.webhook_url = webhook_url
        self.headers = headers or {"Content-Type": "application/json"}
    
    def _send_alert(self, check: HealthCheck, result: Dict[str, Any]) -> None:
        """发送Webhook告警"""
        status = result.get("status")
        message = result.get("message")
        
        # 构建告警数据
        alert_data = {
            "check_name": check.name,
            "middleware_name": check.middleware.name,
            "middleware_type": check.middleware.type,
            "status": status,
            "message": message,
            "timestamp": timezone.now().isoformat(),
            "details": result.get("details", {})
        }
        
        # 发送Webhook请求
        try:
            response = requests.post(
                self.webhook_url,
                headers=self.headers,
                json=alert_data,
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"已发送Webhook告警: {status} - {message}")
        except Exception as e:
            logger.error(f"发送Webhook告警失败: {str(e)}")

# 创建健康监控系统单例
health_monitor = HealthMonitor()

# 启动健康监控系统的函数
def start_health_monitoring():
    """启动健康监控系统"""
    # 如果监控系统已经在运行，直接返回
    if health_monitor.running:
        return
    
    # 从配置中加载告警设置
    if hasattr(settings, 'EMAIL_ALERTS_ENABLED') and settings.EMAIL_ALERTS_ENABLED:
        email_alerter = EmailAlerter(
            smtp_server=settings.EMAIL_HOST,
            smtp_port=settings.EMAIL_PORT,
            sender=settings.EMAIL_HOST_USER,
            password=settings.EMAIL_HOST_PASSWORD,
            recipients=settings.ALERT_EMAIL_RECIPIENTS,
            min_level=settings.ALERT_MIN_LEVEL
        )
        health_monitor.add_alerter(email_alerter)
    
    if hasattr(settings, 'WEBHOOK_ALERTS_ENABLED') and settings.WEBHOOK_ALERTS_ENABLED:
        webhook_alerter = WebhookAlerter(
            webhook_url=settings.WEBHOOK_URL,
            headers=settings.WEBHOOK_HEADERS,
            min_level=settings.ALERT_MIN_LEVEL
        )
        health_monitor.add_alerter(webhook_alerter)
    
    # 启动监控系统
    health_monitor.start()
    logger.info("健康监控系统已启动")

# 停止健康监控系统的函数
def stop_health_monitoring():
    """停止健康监控系统"""
    health_monitor.stop()
    logger.info("健康监控系统已停止")

# 为中间件添加健康检查的函数
def register_middleware_health_check(middleware, adapter, check_interval=60):
    """为中间件注册健康检查"""
    check = MiddlewareHealthCheck(
        middleware=middleware,
        adapter=adapter,
        check_interval=check_interval
    )
    health_monitor.add_check(check)
    logger.info(f"已为中间件 {middleware.id} 注册健康检查")
    return check