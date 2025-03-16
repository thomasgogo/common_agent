from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.shortcuts import get_object_or_404
from django.utils import timezone
from celery import shared_task

from .models import Middleware, MiddlewareConfig, MiddlewareOperation, MiddlewareStatus
from .serializers import (
    MiddlewareSerializer, 
    MiddlewareCreateSerializer,
    MiddlewareOperationSerializer, 
    MiddlewareStatusSerializer,
    MiddlewareUpgradeSerializer,
    MiddlewareConfigUpdateSerializer
)
from .tasks import (
    process_middleware_operation,
    get_middleware_status_info
)


class MiddlewareViewSet(viewsets.ModelViewSet):
    """中间件管理视图集"""
    queryset = Middleware.objects.all()
    permission_classes = [IsAuthenticated]
    
    def get_serializer_class(self):
        if self.action == 'create':
            return MiddlewareCreateSerializer
        return MiddlewareSerializer
    
    @action(detail=True, methods=['get'])
    def status(self, request, pk=None):
        """获取中间件状态"""
        middleware = self.get_object()
        
        # 获取最新状态信息
        status_info = get_middleware_status_info(middleware.id)
        
        # 创建状态记录
        status_record = MiddlewareStatus.objects.create(
            middleware=middleware,
            status=middleware.status,
            **status_info
        )
        
        serializer = MiddlewareStatusSerializer(status_record)
        return Response(serializer.data)
    
    @action(detail=True, methods=['post'])
    def start(self, request, pk=None):
        """启动中间件"""
        middleware = self.get_object()
        
        if middleware.status == 'running':
            return Response(
                {"detail": f"中间件 {middleware.name} 已经在运行中"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # 创建操作记录
        operation = MiddlewareOperation.objects.create(
            middleware=middleware,
            operation_type='start',
            status='pending'
        )
        
        # 异步执行启动操作
        process_middleware_operation.delay(
            str(operation.operation_id),
            'start',
            str(middleware.id)
        )
        
        serializer = MiddlewareOperationSerializer(operation)
        return Response(serializer.data)
    
    @action(detail=True, methods=['post'])
    def stop(self, request, pk=None):
        """停止中间件"""
        middleware = self.get_object()
        
        if middleware.status == 'stopped':
            return Response(
                {"detail": f"中间件 {middleware.name} 已经停止"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # 创建操作记录
        operation = MiddlewareOperation.objects.create(
            middleware=middleware,
            operation_type='stop',
            status='pending'
        )
        
        # 异步执行停止操作
        process_middleware_operation.delay(
            str(operation.operation_id),
            'stop',
            str(middleware.id)
        )
        
        serializer = MiddlewareOperationSerializer(operation)
        return Response(serializer.data)
    
    @action(detail=True, methods=['post'])
    def restart(self, request, pk=None):
        """重启中间件"""
        middleware = self.get_object()
        
        # 创建操作记录
        operation = MiddlewareOperation.objects.create(
            middleware=middleware,
            operation_type='restart',
            status='pending'
        )
        
        # 异步执行重启操作
        process_middleware_operation.delay(
            str(operation.operation_id),
            'restart',
            str(middleware.id)
        )
        
        serializer = MiddlewareOperationSerializer(operation)
        return Response(serializer.data)
    
    @action(detail=True, methods=['post'])
    def upgrade(self, request, pk=None):
        """升级中间件"""
        middleware = self.get_object()
        serializer = MiddlewareUpgradeSerializer(data=request.data)
        
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        # 检查是否已经是目标版本
        if middleware.version == serializer.validated_data['target_version'] and not serializer.validated_data.get('force', False):
            return Response(
                {"detail": f"中间件 {middleware.name} 已经是版本 {serializer.validated_data['target_version']}"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # 创建操作记录
        operation = MiddlewareOperation.objects.create(
            middleware=middleware,
            operation_type='upgrade',
            status='pending',
            params=serializer.validated_data
        )
        
        # 异步执行升级操作
        process_middleware_operation.delay(
            str(operation.operation_id),
            'upgrade',
            str(middleware.id),
            serializer.validated_data
        )
        
        operation_serializer = MiddlewareOperationSerializer(operation)
        return Response(operation_serializer.data)
    
    @action(detail=True, methods=['post'])
    def update_config(self, request, pk=None):
        """更新中间件配置"""
        middleware = self.get_object()
        serializer = MiddlewareConfigUpdateSerializer(data=request.data)
        
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        # 如果仅验证配置而不应用
        if serializer.validated_data.get('validate_only', False):
            # 这里应该根据中间件类型进行配置验证
            # 简化处理，直接返回验证成功
            return Response({"valid": True})
        
        # 创建操作记录
        operation = MiddlewareOperation.objects.create(
            middleware=middleware,
            operation_type='config_update',
            status='pending',
            params=serializer.validated_data
        )
        
        # 异步执行配置更新操作
        process_middleware_operation.delay(
            str(operation.operation_id),
            'config_update',
            str(middleware.id),
            serializer.validated_data
        )
        
        operation_serializer = MiddlewareOperationSerializer(operation)
        return Response(operation_serializer.data)


class OperationViewSet(viewsets.ReadOnlyModelViewSet):
    """中间件操作记录视图集"""
    queryset = MiddlewareOperation.objects.all()
    serializer_class = MiddlewareOperationSerializer
    permission_classes = [IsAuthenticated]
    
    @action(detail=True, methods=['get'])
    def status(self, request, pk=None):
        """获取操作状态"""
        operation = self.get_object()
        serializer = self.get_serializer(operation)
        return Response(serializer.data)
    
    def get_queryset(self):
        queryset = super().get_queryset()
        
        # 支持按中间件ID过滤
        middleware_id = self.request.query_params.get('middleware_id')
        if middleware_id:
            queryset = queryset.filter(middleware_id=middleware_id)
        
        # 支持按操作类型过滤
        operation_type = self.request.query_params.get('operation_type')
        if operation_type:
            queryset = queryset.filter(operation_type=operation_type)
        
        # 支持按状态过滤
        status = self.request.query_params.get('status')
        if status:
            queryset = queryset.filter(status=status)
        
        return queryset