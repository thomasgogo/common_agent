import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from starlette.responses import RedirectResponse
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 创建FastAPI应用实例
app = FastAPI(
    title="通用型网关代理框架",
    description="一个基于Python和FastAPI实现的通用型网关代理框架",
    version="0.1.0",
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 在生产环境中应该限制来源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 根路由重定向到API文档
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

# 导入API路由
from app.api.v1.api import api_router
from app.api.v1.middleware import middleware_router

# 注册API路由
app.include_router(api_router, prefix="/api/v1")
app.include_router(middleware_router, prefix="/api/v1")

# 启动应用
if __name__ == "__main__":
    # 从环境变量获取配置，如果没有则使用默认值
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=True,  # 开发模式下启用热重载
    )