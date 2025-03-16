from rest_framework import serializers
from .models import Middleware, MiddlewareConfig, MiddlewareOperation, MiddlewareStatus


class MiddlewareConfigSerializer(serializers.ModelSerializer):
    """中间件配置序列化器"""
    class Meta:
        model = MiddlewareConfig
        fields = ['config_data']


class MiddlewareSerializer(serializers.ModelSerializer):
    """中间件序列化器"""
    config = MiddlewareConfigSerializer(read_only=True)
    
    class Meta:
        model = Middleware
        fields = ['id', 'name', 'type', 'host', 'port', 'version', 'status', 'last_updated', 'config']
        read_only_fields = ['id', 'last_updated']


class MiddlewareCreateSerializer(serializers.ModelSerializer):
    """中间件创建序列化器"""
    config_data = serializers.JSONField(write_only=True)
    
    class Meta:
        model = Middleware
        fields = ['name', 'type', 'host', 'port', 'version', 'config_data']
    
    def create(self, validated_data):
        config_data = validated_data.pop('config_data')
        middleware = Middleware.objects.create(**validated_data)
        MiddlewareConfig.objects.create(middleware=middleware, config_data=config_data)
        return middleware


class MiddlewareOperationSerializer(serializers.ModelSerializer):
    """中间件操作序列化器"""
    middleware_name = serializers.CharField(source='middleware.name', read_only=True)
    
    class Meta:
        model = MiddlewareOperation
        fields = ['operation_id', 'middleware', 'middleware_name', 'operation_type', 'status', 
                  'params', 'result', 'error_message', 'created_at', 'updated_at', 'completed_at']
        read_only_fields = ['operation_id', 'status', 'result', 'error_message', 'created_at', 'updated_at', 'completed_at']


class MiddlewareStatusSerializer(serializers.ModelSerializer):
    """中间件状态序列化器"""
    middleware_name = serializers.CharField(source='middleware.name', read_only=True)
    
    class Meta:
        model = MiddlewareStatus
        fields = ['middleware', 'middleware_name', 'status', 'uptime', 'connections', 
                  'memory_usage', 'cpu_usage', 'timestamp']
        read_only_fields = ['timestamp']


class MiddlewareUpgradeSerializer(serializers.Serializer):
    """中间件升级序列化器"""
    target_version = serializers.CharField(max_length=20)
    schedule_time = serializers.DateTimeField(required=False, allow_null=True)
    backup = serializers.BooleanField(default=True)
    force = serializers.BooleanField(default=False)


class MiddlewareConfigUpdateSerializer(serializers.Serializer):
    """中间件配置更新序列化器"""
    config = serializers.JSONField()
    restart_after_update = serializers.BooleanField(default=True)
    validate_only = serializers.BooleanField(default=False)