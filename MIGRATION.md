# 权限系统迁移指南

## 适用场景
已部署无权限验证的旧版本，需要升级到带身份认证的新版本。

## 迁移步骤

### 1. 初始化权限系统（仅首次）
```bash
python scripts/init_auth_system.py
```
- 创建默认组织、角色、权限
- 创建 admin 账户（admin / Admin123!@#，可通过 ADMIN_PASSWORD 环境变量覆盖）
- 自动迁移旧文档到 admin

### 2. 迁移文档到 admin 账户
```bash
# 预览（不修改数据）
python scripts/migrate_to_auth_system.py --dry-run

# 执行迁移（默认 public 可见性）
python scripts/migrate_to_auth_system.py

# 机构可见性
python scripts/migrate_to_auth_system.py --visibility organization
```

### 3. 重启服务
```bash
# 停止服务 Ctrl+C
uv run uvicorn web.app:app --host 0.0.0.0 --port 3000 --reload
```

### 4. 完成
- 访问 http://localhost:3000
- 使用 admin 账户登录
- 修改默认密码

## 说明
- 所有旧文档自动归属 admin 账户
- 默认 public 可见性，所有用户可见
- organization 可见性，仅同机构用户可见

