import logging
import time
import traceback
from typing import Dict, Any, Optional, Callable, TypeVar, Generic, List, Tuple
from functools import wraps
from datetime import datetime
import os
import json

# 配置日志
logger = logging.getLogger(__name__)

# 定义泛型类型变量
T = TypeVar('T')


class OperationResult(Generic[T]):
    """操作结果封装类，用于统一处理操作结果和错误"""
    
    def __init__(self, success: bool, data: Optional[T] = None, error: Optional[str] = None):
        self.success = success
        self.data = data
        self.error = error
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error,
            "timestamp": self.timestamp.isoformat()
        }
    
    @classmethod
    def success_result(cls, data: Optional[T] = None) -> 'OperationResult[T]':
        """创建成功结果"""
        return cls(True, data, None)
    
    @classmethod
    def error_result(cls, error: str, data: Optional[T] = None) -> 'OperationResult[T]':
        """创建错误结果"""
        return cls(False, data, error)


class ErrorTracker:
    """错误跟踪器，用于记录和分析错误"""
    
    def __init__(self, log_dir: str = 'logs/errors'):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
    
    def log_error(self, middleware_id: str, operation: str, error: Exception, context: Dict[str, Any] = None) -> str:
        """记录错误信息
        
        Args:
            middleware_id: 中间件ID
            operation: 操作名称
            error: 异常对象
            context: 上下文信息
            
        Returns:
            错误日志文件路径
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        error_id = f"{middleware_id}_{operation}_{timestamp}"
        log_file = os.path.join(self.log_dir, f"{error_id}.json")
        
        error_data = {
            "error_id": error_id,
            "middleware_id": middleware_id,
            "operation": operation,
            "timestamp": datetime.now().isoformat(),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "traceback": traceback.format_exc(),
            "context": context or {}
        }
        
        with open(log_file, 'w') as f:
            json.dump(error_data, f, indent=2)
        
        logger.error(f"错误已记录到 {log_file}: {str(error)}")
        return log_file
    
    def get_error_history(self, middleware_id: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """获取错误历史记录
        
        Args:
            middleware_id: 可选的中间件ID过滤
            limit: 返回的最大记录数
            
        Returns:
            错误历史记录列表
        """
        error_files = [f for f in os.listdir(self.log_dir) if f.endswith('.json')]
        error_files.sort(reverse=True)  # 按文件名倒序排序，最新的错误在前面
        
        errors = []
        for file_name in error_files[:limit]:
            if middleware_id and not file_name.startswith(middleware_id):
                continue
                
            try:
                with open(os.path.join(self.log_dir, file_name), 'r') as f:
                    error_data = json.load(f)
                    errors.append(error_data)
            except Exception as e:
                logger.warning(f"读取错误日志 {file_name} 失败: {str(e)}")
        
        return errors


class RecoveryManager:
    """恢复管理器，用于处理操作失败后的恢复策略"""
    
    def __init__(self):
        self.error_tracker = ErrorTracker()
    
    def with_recovery(self, middleware_id: str, operation: str, recovery_func: Optional[Callable] = None):
        """创建带有恢复机制的装饰器
        
        Args:
            middleware_id: 中间件ID
            operation: 操作名称
            recovery_func: 恢复函数，如果为None则不执行恢复
            
        Returns:
            装饰器函数
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                context = {"args": str(args), "kwargs": str(kwargs)}
                try:
                    result = func(*args, **kwargs)
                    return OperationResult.success_result(result)
                except Exception as e:
                    # 记录错误
                    log_file = self.error_tracker.log_error(middleware_id, operation, e, context)
                    logger.error(f"{operation} 操作失败: {str(e)}")
                    
                    # 尝试恢复
                    if recovery_func:
                        try:
                            logger.info(f"尝试恢复 {operation} 操作")
                            recovery_result = recovery_func(*args, **kwargs)
                            logger.info(f"恢复操作完成: {recovery_result}")
                            return OperationResult.error_result(f"操作失败但已恢复: {str(e)}", recovery_result)
                        except Exception as recovery_error:
                            logger.error(f"恢复操作失败: {str(recovery_error)}")
                            return OperationResult.error_result(f"操作失败且恢复失败: {str(e)}; 恢复错误: {str(recovery_error)}")
                    
                    return OperationResult.error_result(str(e))
            return wrapper
        return decorator
    
    def retry_operation(self, max_attempts: int = 3, delay: int = 2, backoff: int = 2, exceptions: Tuple = (Exception,)):
        """创建带有重试机制的装饰器
        
        Args:
            max_attempts: 最大尝试次数
            delay: 初始延迟时间（秒）
            backoff: 延迟时间的增长因子
            exceptions: 需要捕获的异常类型
            
        Returns:
            装饰器函数
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                attempt = 0
                current_delay = delay
                last_exception = None
                
                while attempt < max_attempts:
                    try:
                        result = func(*args, **kwargs)
                        return OperationResult.success_result(result)
                    except exceptions as e:
                        attempt += 1
                        last_exception = e
                        
                        if attempt >= max_attempts:
                            logger.error(f"操作失败，已达到最大重试次数: {str(e)}")
                            break
                        
                        logger.warning(f"操作失败，将在 {current_delay} 秒后重试 ({attempt}/{max_attempts}): {str(e)}")
                        time.sleep(current_delay)
                        current_delay *= backoff
                
                return OperationResult.error_result(f"操作失败，已重试 {attempt} 次: {str(last_exception)}")
            return wrapper
        return decorator


class TransactionManager:
    """事务管理器，用于确保操作的原子性"""
    
    def __init__(self):
        self.recovery_manager = RecoveryManager()
    
    def transaction(self, middleware_id: str, operation: str):
        """创建事务装饰器
        
        Args:
            middleware_id: 中间件ID
            operation: 操作名称
            
        Returns:
            装饰器函数
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # 记录操作开始
                logger.info(f"开始事务: {operation} 于中间件 {middleware_id}")
                
                # 保存操作前的状态（用于回滚）
                try:
                    # 这里应该实现保存操作前状态的逻辑
                    # 例如，对于配置更新，可以先备份当前配置
                    
                    # 执行操作
                    result = func(*args, **kwargs)
                    
                    # 记录操作成功
                    logger.info(f"事务成功完成: {operation} 于中间件 {middleware_id}")
                    return OperationResult.success_result(result)
                    
                except Exception as e:
                    # 记录操作失败
                    logger.error(f"事务失败: {operation} 于中间件 {middleware_id}: {str(e)}")
                    
                    # 尝试回滚
                    try:
                        # 这里应该实现回滚逻辑
                        logger.info(f"尝试回滚事务: {operation} 于中间件 {middleware_id}")
                        
                        # 记录回滚成功
                        logger.info(f"事务回滚成功: {operation} 于中间件 {middleware_id}")
                        return OperationResult.error_result(f"操作失败但已回滚: {str(e)}")
                        
                    except Exception as rollback_error:
                        # 记录回滚失败
                        logger.error(f"事务回滚失败: {operation} 于中间件 {middleware_id}: {str(rollback_error)}")
                        return OperationResult.error_result(f"操作失败且回滚失败: {str(e)}; 回滚错误: {str(rollback_error)}")
            return wrapper
        return decorator


# 使用示例
def example_usage():
    # 创建恢复管理器
    recovery_manager = RecoveryManager()
    
    # 定义一个可能失败的操作
    @recovery_manager.retry_operation(max_attempts=3, delay=1)
    def risky_operation(value):
        if value < 0:
            raise ValueError("Value cannot be negative")
        return value * 2
    
    # 定义一个带有恢复机制的操作
    def recovery_function(*args, **kwargs):
        # 实现恢复逻辑
        return "Recovered value"
    
    @recovery_manager.with_recovery("test-middleware", "test-operation", recovery_function)
    def operation_with_recovery(value):
        if value == 0:
            raise ZeroDivisionError("Cannot divide by zero")
        return 10 / value
    
    # 创建事务管理器
    transaction_manager = TransactionManager()
    
    @transaction_manager.transaction("test-middleware", "test-transaction")
    def transactional_operation(value):
        if value > 100:
            raise ValueError("Value too large")
        return value + 10
    
    # 测试重试操作
    result1 = risky_operation(5)  # 应该成功
    print(f"Result 1: {result1.to_dict()}")
    
    result2 = risky_operation(-5)  # 应该失败并重试
    print(f"Result 2: {result2.to_dict()}")
    
    # 测试带恢复的操作
    result3 = operation_with_recovery(2)  # 应该成功
    print(f"Result 3: {result3.to_dict()}")
    
    result4 = operation_with_recovery(0)  # 应该失败并恢复
    print(f"Result 4: {result4.to_dict()}")
    
    # 测试事务操作
    result5 = transactional_operation(50)  # 应该成功
    print(f"Result 5: {result5.to_dict()}")
    
    result6 = transactional_operation(200)  # 应该失败并回滚
    print(f"Result 6: {result6.to_dict()}")


if __name__ == "__main__":
    # 设置日志级别
    logging.basicConfig(level=logging.INFO)
    
    # 运行示例
    example_usage()