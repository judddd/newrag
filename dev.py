import subprocess
import signal
import sys
import os
import time
import threading
import yaml # 需要 PyYAML，通常项目中已有
from pathlib import Path

# Ensure logs directory exists
Path("logs").mkdir(exist_ok=True)

def load_config():
    """读取 config.yaml 配置"""
    try:
        with open("config.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            web_conf = config.get("web", {})
            mcp_conf = config.get("mcp", {})
            security_conf = config.get("security", {})
            return {
                "backend_host": web_conf.get("host", "0.0.0.0"),
                "backend_port": web_conf.get("port", 8080),
                "frontend_port": web_conf.get("frontend_port", 3000),
                "mcp_host": mcp_conf.get("host", "0.0.0.0"),
                "mcp_port": mcp_conf.get("port", 3001),
                "jwt_secret": security_conf.get("jwt_secret", ""),
            }
    except Exception as e:
        print(f"⚠️  读取 config.yaml 失败，使用默认配置: {e}")
        return {
            "backend_host": "0.0.0.0",
            "backend_port": 8080,
            "frontend_port": 3000,
            "mcp_host": "0.0.0.0",
            "mcp_port": 3001,
            "jwt_secret": "",
        }

def print_ready_message(conf):
    """打印就绪信息"""
    time.sleep(5) # 增加一点延迟等待 MCP 启动
    
    # 处理 host 显示，0.0.0.0 换成 localhost 以便点击
    backend_host_display = "localhost" if conf["backend_host"] == "0.0.0.0" else conf["backend_host"]
    mcp_host_display = "localhost" if conf["mcp_host"] == "0.0.0.0" else conf["mcp_host"]
    
    print("\n" + "="*60)
    print("🚀  NewRAG - 开发环境已就绪")
    print("="*60)
    print(f"\n👉  前端访问地址 (React):  http://localhost:{conf['frontend_port']}")
    print(f"    后端 API 地址:         http://{backend_host_display}:{conf['backend_port']}")
    print(f"    MCP 服务地址:          http://{mcp_host_display}:{conf['mcp_port']}/mcp")
    print(f"\n    配置已从 config.yaml 加载")
    print("\n" + "="*60 + "\n")

def main():
    root_dir = os.getcwd()
    frontend_dir = os.path.join(root_dir, "frontend")
    mcp_dir = os.path.join(root_dir, "newrag-mcp")
    
    # 1. 加载配置
    conf = load_config()

    print(f"🚀 正在启动 NewRAG (Frontend: {conf['frontend_port']}, Backend: {conf['backend_port']}, MCP: {conf['mcp_port']})...")

    npm_cmd = "npm"
    if os.name == 'nt':
        npm_cmd = "npm.cmd"

    # 2. 启动 MCP 服务
    print("🤖 启动 MCP 服务...")
    mcp_env = os.environ.copy()
    # 传递配置给 MCP
    # 如果配置是 0.0.0.0，我们让 MCP 监听所有接口，但告诉前端和显示时使用 localhost
    mcp_bind_host = conf["mcp_host"]
    mcp_display_host = "localhost" if mcp_bind_host == "0.0.0.0" else mcp_bind_host
    
    mcp_env["MCP_HTTP_PORT"] = str(conf["mcp_port"])
    mcp_env["MCP_HTTP_HOST"] = mcp_bind_host
    # 确保 MCP 拿到和后端一致的 JWT Secret
    jwt_secret = os.getenv("JWT_SECRET") or conf.get("jwt_secret", "")
    if jwt_secret:
        mcp_env["JWT_SECRET"] = jwt_secret
    
    # 检查是否需要安装依赖或构建
    if not os.path.exists(os.path.join(mcp_dir, "node_modules")):
        print("📦 安装 MCP 依赖...")
        subprocess.run([npm_cmd, "install"], cwd=mcp_dir, check=True)
    
    if not os.path.exists(os.path.join(mcp_dir, "dist")):
        print("🔨 构建 MCP 服务...")
        subprocess.run([npm_cmd, "run", "build"], cwd=mcp_dir, check=True)

    mcp_process = subprocess.Popen(
        [npm_cmd, "run", "start:http"],
        cwd=mcp_dir,
        env=mcp_env
    )

    # 3. 启动后端
    print("📦 启动后端服务...")
    # 后端通常自己会读 config.yaml，所以不需要传 env，除非想覆盖
    backend_env = os.environ.copy()
    backend_process = subprocess.Popen(
        [sys.executable, "web/app.py"],
        cwd=root_dir,
        env=backend_env
    )

    # 4. 启动前端
    print("🎨 启动前端服务...")
    frontend_env = os.environ.copy()
    
    # 关键：将配置注入环境变量，供 vite.config.ts 读取
    frontend_env["FRONTEND_PORT"] = str(conf["frontend_port"])
    # 构造后端 URL 供代理使用
    backend_host = "localhost" if conf["backend_host"] == "0.0.0.0" else conf["backend_host"]
    frontend_env["BACKEND_URL"] = f"http://{backend_host}:{conf['backend_port']}"
    # 注入 MCP URL 供前端使用
    frontend_env["VITE_MCP_URL"] = f"http://{mcp_display_host}:{conf['mcp_port']}/mcp"
    frontend_env["VITE_MCP_HOST_DISPLAY"] = mcp_display_host
    frontend_env["VITE_MCP_PORT"] = str(conf["mcp_port"])
        
    frontend_process = subprocess.Popen(
        [npm_cmd, "run", "dev"],
        cwd=frontend_dir,
        env=frontend_env
    )

    threading.Thread(target=print_ready_message, args=(conf,), daemon=True).start()

    def cleanup(signum, frame):
        print("\n🛑 正在停止服务...")
        if frontend_process.poll() is None:
            try: frontend_process.terminate() 
            except: pass
        if backend_process.poll() is None:
            try: backend_process.terminate()
            except: pass
        if mcp_process.poll() is None:
            try: mcp_process.terminate()
            except: pass
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    try:
        backend_process.wait()
        frontend_process.wait()
        mcp_process.wait()
    except KeyboardInterrupt:
        cleanup(None, None)

if __name__ == "__main__":
    main()
