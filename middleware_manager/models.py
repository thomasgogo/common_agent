from django.db import models
from django.utils import timezone
import uuid


class Middleware(models.Model):
    """中间件模型，存储中间件基本信息"""
    MIDDLEWARE_TYPES = [
        ('redis', 'Redis'),
        ('mysql', 'MySQL'),
        ('mongodb', 'MongoDB'),
        ('elasticsearch', 'Elasticsearch'),
        ('rabbitmq', 'RabbitMQ'),
    ]
    
    STATUS_CHOICES = [
        ('running', '运行中'),
        ('stopped', '已停止'),
        ('updating', '更新中'),
        ('error', '错误'),
    ]
    
    id = models.CharField(primary_key=True, max_length=50, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=100, verbose_name='中间件名称')
    type = models.CharField(max_length=20, choices=MIDDLEWARE_TYPES, verbose_name='中间件类型')
    host = models.CharField(max_length=100, verbose_name='主机地址')
    port = models.IntegerField(verbose_name='端口')
    version = models.CharField(max_length=20, verbose_name='版本')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='stopped', verbose_name='状态')
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    last_updated = models.DateTimeField(auto_now=True, verbose_name='最后更新时间')
    
    class Meta:
        verbose_name = '中间件'
        verbose_name_plural = '中间件'
        ordering = ['-last_updated']
    
    def __str__(self):
        return f"{self.name} ({self.type})"


class MiddlewareConfig(models.Model):
    """中间件配置模型，存储中间件的配置信息"""
    middleware = models.OneToOneField(Middleware, on_delete=models.CASCADE, related_name='config', verbose_name='中间件')
    config_data = models.JSONField(verbose_name='配置数据')
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='更新时间')
    
    class Meta:
        verbose_name = '中间件配置'
        verbose_name_plural = '中间件配置'
    
    def __str__(self):
        return f"{self.middleware.name} 配置"


class MiddlewareOperation(models.Model):
    """中间件操作记录模型，记录对中间件的操作历史"""
    OPERATION_TYPES = [
        ('start', '启动'),
        ('stop', '停止'),
        ('restart', '重启'),
        ('upgrade', '升级'),
        ('config_update', '配置更新'),
    ]
    
    STATUS_CHOICES = [
        ('pending', '等待中'),
        ('in_progress', '进行中'),
        ('completed', '已完成'),
        ('failed', '失败'),
    ]
    
    operation_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    middleware = models.ForeignKey(Middleware, on_delete=models.CASCADE, related_name='operations', verbose_name='中间件')
    operation_type = models.CharField(max_length=20, choices=OPERATION_TYPES, verbose_name='操作类型')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending', verbose_name='状态')
    params = models.JSONField(null=True, blank=True, verbose_name='操作参数')
    result = models.JSONField(null=True, blank=True, verbose_name='操作结果')
    error_message = models.TextField(null=True, blank=True, verbose_name='错误信息')
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='创建时间')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='更新时间')
    completed_at = models.DateTimeField(null=True, blank=True, verbose_name='完成时间')
    
    class Meta:
        verbose_name = '中间件操作'
        verbose_name_plural = '中间件操作'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.middleware.name} - {self.get_operation_type_display()} ({self.get_status_display()})"
    
    def mark_completed(self, result=None):
        """标记操作为已完成"""
        self.status = 'completed'
        self.result = result or {'success': True}
        self.completed_at = timezone.now()
        self.save()
    
    def mark_failed(self, error_message):
        """标记操作为失败"""
        self.status = 'failed'
        self.error_message = error_message
        self.completed_at = timezone.now()
        self.save()


class MiddlewareStatus(models.Model):
    """中间件状态记录模型，记录中间件的实时状态信息"""
    middleware = models.ForeignKey(Middleware, on_delete=models.CASCADE, related_name='status_history', verbose_name='中间件')
    status = models.CharField(max_length=20, choices=Middleware.STATUS_CHOICES, verbose_name='状态')
    uptime = models.IntegerField(null=True, blank=True, verbose_name='运行时间(秒)')
    connections = models.IntegerField(null=True, blank=True, verbose_name='连接数')
    memory_usage = models.FloatField(null=True, blank=True, verbose_name='内存使用(MB)')
    cpu_usage = models.FloatField(null=True, blank=True, verbose_name='CPU使用率(%)')
    timestamp = models.DateTimeField(auto_now_add=True, verbose_name='记录时间')
    
    class Meta:
        verbose_name = '中间件状态'
        verbose_name_plural = '中间件状态'
        ordering = ['-timestamp']
    
    def __str__(self):
        return f"{self.middleware.name} 状态 - {self.timestamp}"