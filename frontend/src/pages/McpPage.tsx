import { useState, useEffect } from 'react';
import { Server, Copy, Terminal, BookOpen, Key, Plus, Trash2, Check } from 'lucide-react';
import { listMcpTokens, createMcpToken, deleteMcpToken, type McpToken } from '../api/mcp';

export default function McpPage() {
  const mcpHost = import.meta.env.VITE_MCP_HOST_DISPLAY || 'localhost';
  const mcpPort = import.meta.env.VITE_MCP_PORT || '3001';
  const mcpUrl = `http://${mcpHost}:${mcpPort}/mcp`;

  const [tokens, setTokens] = useState<McpToken[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [newTokenName, setNewTokenName] = useState('');
  const [newTokenExpiry, setNewTokenExpiry] = useState('never');
  const [copiedId, setCopiedId] = useState<number | null>(null);

  useEffect(() => {
    loadTokens();
  }, []);

  const loadTokens = async () => {
    try {
      const data = await listMcpTokens();
      setTokens(data);
    } catch (error) {
      console.error('Failed to load MCP tokens:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleCreateToken = async () => {
    if (!newTokenName.trim()) {
      alert('请输入 Token 名称');
      return;
    }

    setCreating(true);
    try {
      const expiresIn = newTokenExpiry === 'never' ? undefined : parseInt(newTokenExpiry);
      const token = await createMcpToken({
        name: newTokenName,
        expires_days: expiresIn,
      });
      
      setTokens([token, ...tokens]);
      setNewTokenName('');
      setNewTokenExpiry('never');
      
      // Auto-copy newly created token
      copyTokenWithUrl(token.token, token.id);
    } catch (error: any) {
      alert(`创建失败: ${error.message}`);
    } finally {
      setCreating(false);
    }
  };

  const handleDeleteToken = async (tokenId: number) => {
    if (!confirm('确定要删除这个 Token 吗？此操作不可恢复。')) {
      return;
    }

    try {
      await deleteMcpToken(tokenId);
      setTokens(tokens.filter(t => t.id !== tokenId));
    } catch (error: any) {
      alert(`删除失败: ${error.message}`);
    }
  };

  const copyTokenWithUrl = (token: string, tokenId: number) => {
    const configJson = {
      "mcpServers": {
        "newrag": {
          "command": "node",
          "args": [],
          "transport": {
            "type": "http",
            "url": mcpUrl,
            "headers": {
              "Authorization": `Bearer ${token}`
            }
          }
        }
      }
    };
    
    navigator.clipboard.writeText(JSON.stringify(configJson, null, 2));
    setCopiedId(tokenId);
    setTimeout(() => setCopiedId(null), 2000);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return '从未使用';
    return new Date(dateStr).toLocaleString('zh-CN');
  };

  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      {/* Header */}
      <div className="flex flex-col gap-2">
        <h1 className="text-3xl font-bold text-slate-900 dark:text-white flex items-center gap-3">
          <Server className="w-8 h-8 text-indigo-600 dark:text-indigo-400" />
          MCP 服务信息
        </h1>
        <p className="text-slate-600 dark:text-slate-400 text-lg">
          Model Context Protocol (MCP) 服务端点配置与令牌管理
        </p>
      </div>

      {/* Connection Info Card */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden">
        <div className="p-6 border-b border-slate-200 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-900/50">
          <h2 className="text-lg font-semibold text-slate-900 dark:text-white flex items-center gap-2">
            <Terminal className="w-5 h-5 text-slate-500" />
            连接信息
          </h2>
        </div>
        <div className="p-6 space-y-6">
          <div className="grid gap-6 md:grid-cols-2">
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-500 dark:text-slate-400">
                MCP Streamable HTTP 端点 URL
              </label>
              <div className="flex items-center gap-2">
                <code className="flex-1 p-3 bg-slate-100 dark:bg-slate-800 rounded-lg text-slate-900 dark:text-slate-100 font-mono text-sm border border-slate-200 dark:border-slate-700">
                  {mcpUrl}
                </code>
                <button
                  onClick={() => copyToClipboard(mcpUrl)}
                  className="p-2.5 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg text-slate-500 hover:text-indigo-600 dark:text-slate-400 dark:hover:text-indigo-400 transition-colors border border-transparent hover:border-slate-200 dark:hover:border-slate-700"
                  title="复制 URL"
                >
                  <Copy size={18} />
                </button>
              </div>
              <p className="text-xs text-slate-500">
                在 MCP 客户端中使用此 URL，配合下方生成的 Token 进行连接。
              </p>
            </div>
            
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-500 dark:text-slate-400">
                服务状态
              </label>
              <div className="flex items-center gap-3 p-3 bg-green-50 dark:bg-green-900/20 border border-green-100 dark:border-green-900/30 rounded-lg">
                <div className="relative flex h-3 w-3">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
                </div>
                <span className="text-green-700 dark:text-green-400 font-medium text-sm">
                  运行中 (Port {mcpPort})
                </span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* MCP Token Management */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm overflow-hidden">
        <div className="p-6 border-b border-slate-200 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-900/50">
          <h2 className="text-lg font-semibold text-slate-900 dark:text-white flex items-center gap-2">
            <Key className="w-5 h-5 text-slate-500" />
            MCP 访问令牌
          </h2>
          <p className="text-sm text-slate-600 dark:text-slate-400 mt-1">
            创建 Token 后自动复制完整 JSON 配置，直接粘贴到 Cursor / Claude Desktop
          </p>
        </div>

        {/* Create Token Form */}
        <div className="p-6 border-b border-slate-200 dark:border-slate-800 bg-slate-50/30 dark:bg-slate-900/30">
          <div className="flex gap-3 flex-wrap">
            <input
              type="text"
              placeholder="Token 名称 (如：我的笔记本)"
              value={newTokenName}
              onChange={(e) => setNewTokenName(e.target.value)}
              className="flex-1 min-w-[200px] px-4 py-2 bg-white dark:bg-slate-800 border border-slate-300 dark:border-slate-700 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 dark:text-white"
            />
            <select
              value={newTokenExpiry}
              onChange={(e) => setNewTokenExpiry(e.target.value)}
              className="px-4 py-2 bg-white dark:bg-slate-800 border border-slate-300 dark:border-slate-700 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 dark:text-white"
            >
              <option value="never">永不过期</option>
              <option value="7">7 天</option>
              <option value="30">30 天</option>
              <option value="90">90 天</option>
              <option value="365">1 年</option>
            </select>
            <button
              onClick={handleCreateToken}
              disabled={creating}
              className="px-6 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:bg-slate-400 text-white rounded-lg font-medium transition-colors flex items-center gap-2"
            >
              <Plus size={18} />
              {creating ? '创建中...' : '创建 Token'}
            </button>
          </div>
        </div>

        {/* Token List */}
        <div className="divide-y divide-slate-200 dark:divide-slate-800">
          {loading ? (
            <div className="p-8 text-center text-slate-500">
              加载中...
            </div>
          ) : tokens.length === 0 ? (
            <div className="p-8 text-center text-slate-500">
              暂无 Token，请创建一个
            </div>
          ) : (
            tokens.map((token) => (
              <div key={token.id} className="p-6 hover:bg-slate-50/50 dark:hover:bg-slate-800/30 transition-colors">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-3 mb-2">
                      <h3 className="text-base font-semibold text-slate-900 dark:text-white">
                        {token.name}
                      </h3>
                      <span className="px-2 py-0.5 bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400 text-xs font-medium rounded">
                        活跃
                      </span>
                    </div>
                    <div className="space-y-1 text-sm text-slate-600 dark:text-slate-400">
                      <div>创建时间: {formatDate(token.created_at)}</div>
                      <div>最后使用: {formatDate(token.last_used)}</div>
                      {token.expires_at && (
                        <div>过期时间: {formatDate(token.expires_at)}</div>
                      )}
                    </div>
                    <div className="mt-3">
                      <code className="block p-2 bg-slate-100 dark:bg-slate-800 rounded text-xs font-mono text-slate-700 dark:text-slate-300 break-all">
                        {token.token || '**************************************************************** (旧 Token 无法查看)'}
                      </code>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    {token.token && (
                      <button
                        onClick={() => copyTokenWithUrl(token.token, token.id)}
                        className="p-2 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg text-slate-500 hover:text-indigo-600 dark:text-slate-400 dark:hover:text-indigo-400 transition-colors"
                        title="复制完整 JSON 配置"
                      >
                        {copiedId === token.id ? (
                          <Check size={18} className="text-green-600" />
                        ) : (
                          <Copy size={18} />
                        )}
                      </button>
                    )}
                    <button
                      onClick={() => handleDeleteToken(token.id)}
                      className="p-2 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-lg text-slate-500 hover:text-red-600 dark:text-slate-400 dark:hover:text-red-400 transition-colors"
                      title="删除 Token"
                    >
                      <Trash2 size={18} />
                    </button>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Configuration Guide */}
      <div className="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm">
        <div className="p-6 border-b border-slate-200 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-900/50">
          <h2 className="text-lg font-semibold text-slate-900 dark:text-white flex items-center gap-2">
            <BookOpen className="w-5 h-5 text-slate-500" />
            配置指南
          </h2>
        </div>
        <div className="p-6 space-y-4">
          <div className="space-y-2">
            <h3 className="text-sm font-semibold text-slate-900 dark:text-white">
              Cursor / Claude Desktop 配置
            </h3>
            <p className="text-sm text-slate-600 dark:text-slate-400">
              点击 Token 旁的复制按钮，自动复制配置 JSON，粘贴到 MCP 配置文件：
            </p>
            <div className="bg-slate-50 dark:bg-slate-800/50 rounded-lg p-4 border border-slate-200 dark:border-slate-700">
              <code className="text-xs font-mono text-slate-700 dark:text-slate-300 whitespace-pre-wrap break-all">
{`{
  "mcpServers": {
    "newrag": {
      "command": "node",
      "args": [],
      "transport": {
        "type": "http",
        "url": "${mcpUrl}",
        "headers": {
          "Authorization": "Bearer YOUR_TOKEN_HERE"
        }
      }
    }
  }
}`}
              </code>
            </div>
          </div>

          <div className="p-4 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg">
            <p className="text-sm text-amber-900 dark:text-amber-300 font-medium mb-1">
              🔐 安全提示
            </p>
            <p className="text-xs text-amber-800 dark:text-amber-400">
              Token 是长期凭证，请妥善保管。为每个设备创建独立 Token，不用时及时删除。
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
