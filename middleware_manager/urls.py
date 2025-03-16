from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import MiddlewareViewSet, OperationViewSet

# 创建路由器并注册视图集
router = DefaultRouter()
router.register(r'middlewares', MiddlewareViewSet)
router.register(r'operations', OperationViewSet)

# API URL配置
urlpatterns = [
    # 包含自动生成的API路由
    path('', include(router.urls)),
]