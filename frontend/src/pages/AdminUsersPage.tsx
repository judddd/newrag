/**
 * Admin Users Management Page
 * 
 * Allows administrators to manage users
 */

import { useState, useEffect } from 'react';
import { Users, Plus, Edit, Key, Ban, Search } from 'lucide-react';
import {
  listUsers,
  createUser,
  updateUser,
  deleteUser,
  resetUserPassword,
  listOrganizations,
  listRoles,
  type User,
  type CreateUserRequest,
  type UpdateUserRequest,
  type Organization,
  type Role
} from '../api/admin';

export default function AdminUsersPage() {
  const [users, setUsers] = useState<User[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [perPage] = useState(20);
  const [loading, setLoading] = useState(false);
  
  const [search, setSearch] = useState('');
  const [filterOrgId, setFilterOrgId] = useState<number | undefined>();
  const [filterRoleCode, setFilterRoleCode] = useState<string | undefined>();
  const [filterIsActive, setFilterIsActive] = useState<boolean | undefined>();
  
  const [organizations, setOrganizations] = useState<Organization[]>([]);
  const [roles, setRoles] = useState<Role[]>([]);
  
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  const [selectedUser, setSelectedUser] = useState<User | null>(null);
  
  useEffect(() => {
    loadUsers();
    loadOrganizations();
    loadRoles();
  }, [page, search, filterOrgId, filterRoleCode, filterIsActive]);
  
  const loadUsers = async () => {
    setLoading(true);
    try {
      const data = await listUsers({
        page,
        per_page: perPage,
        search: search || undefined,
        org_id: filterOrgId,
        role_code: filterRoleCode,
        is_active: filterIsActive
      });
      setUsers(data.items);
      setTotal(data.total);
    } catch (error: any) {
      console.error('Failed to load users:', error);
      alert(`加载用户失败: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };
  
  const loadOrganizations = async () => {
    try {
      const data = await listOrganizations();
      setOrganizations(data);
    } catch (error: any) {
      console.error('Failed to load organizations:', error);
    }
  };
  
  const loadRoles = async () => {
    try {
      const data = await listRoles();
      setRoles(data);
    } catch (error: any) {
      console.error('Failed to load roles:', error);
    }
  };
  
  const handleEditUser = (user: User) => {
    setSelectedUser(user);
    setShowEditModal(true);
  };
  
  const handleResetPassword = async (user: User) => {
    const newPassword = prompt(`为用户 ${user.username} 重置密码\n\n请输入新密码 (至少8个字符):`);
    if (!newPassword) return;
    
    if (newPassword.length < 8) {
      alert('密码长度至少为8个字符');
      return;
    }
    
    try {
      await resetUserPassword(user.id, { new_password: newPassword });
      alert('密码重置成功');
    } catch (error: any) {
      alert(`密码重置失败: ${error.response?.data?.detail || error.message}`);
    }
  };
  
  const handleDisableUser = async (user: User) => {
    if (!confirm(`确定要${user.is_active ? '禁用' : '启用'}用户 ${user.username}?`)) {
      return;
    }
    
    try {
      if (user.is_active) {
        await deleteUser(user.id);
        alert('用户已禁用');
      } else {
        await updateUser(user.id, { is_active: true });
        alert('用户已启用');
      }
      loadUsers();
    } catch (error: any) {
      alert(`操作失败: ${error.response?.data?.detail || error.message}`);
    }
  };
  
  const totalPages = Math.ceil(total / perPage);
  
  return (
    <div className="min-h-screen bg-slate-50 dark:bg-slate-900 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-slate-900 dark:text-white flex items-center gap-3">
              <Users size={32} className="text-indigo-600" />
              用户管理
            </h1>
            <p className="text-slate-600 dark:text-slate-400 mt-1">
              管理系统用户、角色和权限
            </p>
          </div>
          <button
            onClick={() => setShowCreateModal(true)}
            className="flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 transition-colors"
          >
            <Plus size={20} />
            创建用户
          </button>
        </div>
        
        {/* Filters */}
        <div className="bg-white dark:bg-slate-800 rounded-lg shadow-md p-4 mb-6">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-slate-400" size={18} />
              <input
                type="text"
                placeholder="搜索用户名或邮箱..."
                value={search}
                onChange={(e) => {
                  setSearch(e.target.value);
                  setPage(1);
                }}
                className="w-full pl-10 pr-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg focus:ring-2 focus:ring-indigo-500 dark:bg-slate-700 dark:text-white"
              />
            </div>
            
            <select
              value={filterOrgId || ''}
              onChange={(e) => {
                setFilterOrgId(e.target.value ? parseInt(e.target.value) : undefined);
                setPage(1);
              }}
              className="px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg focus:ring-2 focus:ring-indigo-500 dark:bg-slate-700 dark:text-white"
            >
              <option value="">所有组织</option>
              {organizations.map(org => (
                <option key={org.id} value={org.id}>{org.name}</option>
              ))}
            </select>
            
            <select
              value={filterRoleCode || ''}
              onChange={(e) => {
                setFilterRoleCode(e.target.value || undefined);
                setPage(1);
              }}
              className="px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg focus:ring-2 focus:ring-indigo-500 dark:bg-slate-700 dark:text-white"
            >
              <option value="">所有角色</option>
              {roles.map(role => (
                <option key={role.code} value={role.code}>{role.name}</option>
              ))}
            </select>
            
            <select
              value={filterIsActive === undefined ? '' : filterIsActive ? 'active' : 'inactive'}
              onChange={(e) => {
                setFilterIsActive(e.target.value === '' ? undefined : e.target.value === 'active');
                setPage(1);
              }}
              className="px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg focus:ring-2 focus:ring-indigo-500 dark:bg-slate-700 dark:text-white"
            >
              <option value="">所有状态</option>
              <option value="active">活跃</option>
              <option value="inactive">已禁用</option>
            </select>
          </div>
        </div>
        
        {/* Users Table */}
        <div className="bg-white dark:bg-slate-800 rounded-lg shadow-md overflow-hidden">
          {loading ? (
            <div className="text-center py-12">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-600 mx-auto"></div>
              <p className="mt-4 text-slate-600 dark:text-slate-400">加载中...</p>
            </div>
          ) : users.length === 0 ? (
            <div className="text-center py-12">
              <Users size={48} className="mx-auto text-slate-400 mb-4" />
              <p className="text-slate-600 dark:text-slate-400">暂无用户</p>
            </div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-slate-50 dark:bg-slate-700">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-300 uppercase tracking-wider">
                        用户名
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-300 uppercase tracking-wider">
                        邮箱
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-300 uppercase tracking-wider">
                        组织
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-300 uppercase tracking-wider">
                        角色
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-300 uppercase tracking-wider">
                        状态
                      </th>
                      <th className="px-6 py-3 text-right text-xs font-medium text-slate-500 dark:text-slate-300 uppercase tracking-wider">
                        操作
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200 dark:divide-slate-700">
                    {users.map(user => (
                      <tr key={user.id} className="hover:bg-slate-50 dark:hover:bg-slate-700/50">
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="flex items-center">
                            <div>
                              <div className="text-sm font-medium text-slate-900 dark:text-white">
                                {user.username}
                                {user.is_superuser && (
                                  <span className="ml-2 px-2 py-0.5 text-xs bg-red-100 text-red-800 rounded-full">
                                    管理员
                                  </span>
                                )}
                              </div>
                              <div className="text-xs text-slate-500 dark:text-slate-400">
                                ID: {user.id}
                              </div>
                            </div>
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-900 dark:text-white">
                          {user.email}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-slate-900 dark:text-white">
                          {user.org_name || '-'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="flex flex-wrap gap-1">
                            {user.roles.map(role => (
                              <span
                                key={role.code}
                                className="px-2 py-1 text-xs bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-200 rounded"
                              >
                                {role.name}
                              </span>
                            ))}
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={`px-2 py-1 text-xs rounded-full ${
                            user.is_active
                              ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                              : 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                          }`}>
                            {user.is_active ? '活跃' : '已禁用'}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                          <div className="flex items-center justify-end gap-2">
                            <button
                              onClick={() => handleEditUser(user)}
                              className="text-indigo-600 hover:text-indigo-900 dark:text-indigo-400 dark:hover:text-indigo-300"
                              title="编辑"
                            >
                              <Edit size={18} />
                            </button>
                            <button
                              onClick={() => handleResetPassword(user)}
                              className="text-yellow-600 hover:text-yellow-900 dark:text-yellow-400 dark:hover:text-yellow-300"
                              title="重置密码"
                            >
                              <Key size={18} />
                            </button>
                            <button
                              onClick={() => handleDisableUser(user)}
                              className="text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300"
                              title={user.is_active ? '禁用' : '启用'}
                            >
                              <Ban size={18} />
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              
              {/* Pagination */}
              <div className="bg-slate-50 dark:bg-slate-700 px-6 py-4 flex items-center justify-between border-t border-slate-200 dark:border-slate-600">
                <div className="text-sm text-slate-700 dark:text-slate-300">
                  显示 {(page - 1) * perPage + 1} - {Math.min(page * perPage, total)} / 共 {total} 个用户
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => setPage(p => Math.max(1, p - 1))}
                    disabled={page === 1}
                    className="px-3 py-1 border border-slate-300 dark:border-slate-600 rounded hover:bg-slate-100 dark:hover:bg-slate-600 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    上一页
                  </button>
                  <span className="px-3 py-1 text-slate-700 dark:text-slate-300">
                    {page} / {totalPages}
                  </span>
                  <button
                    onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                    disabled={page === totalPages}
                    className="px-3 py-1 border border-slate-300 dark:border-slate-600 rounded hover:bg-slate-100 dark:hover:bg-slate-600 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    下一页
                  </button>
                </div>
              </div>
            </>
          )}
        </div>
      </div>
      
      {/* Modals */}
      {showCreateModal && (
        <CreateUserModal
          organizations={organizations}
          roles={roles}
          onClose={() => setShowCreateModal(false)}
          onSuccess={() => {
            setShowCreateModal(false);
            loadUsers();
          }}
        />
      )}
      
      {showEditModal && selectedUser && (
        <EditUserModal
          user={selectedUser}
          organizations={organizations}
          roles={roles}
          onClose={() => {
            setShowEditModal(false);
            setSelectedUser(null);
          }}
          onSuccess={() => {
            setShowEditModal(false);
            setSelectedUser(null);
            loadUsers();
          }}
        />
      )}
    </div>
  );
}

// Create User Modal
function CreateUserModal({
  organizations,
  roles,
  onClose,
  onSuccess
}: {
  organizations: Organization[];
  roles: Role[];
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [formData, setFormData] = useState<CreateUserRequest>({
    username: '',
    email: '',
    password: '',
    org_id: organizations[0]?.id || 0,
    role_codes: ['viewer'],
    is_active: true,
    is_superuser: false
  });
  const [saving, setSaving] = useState(false);
  
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    
    try {
      await createUser(formData);
      alert('用户创建成功');
      onSuccess();
    } catch (error: any) {
      alert(`创建失败: ${error.response?.data?.detail || error.message}`);
    } finally {
      setSaving(false);
    }
  };
  
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-md p-6">
        <h2 className="text-xl font-bold text-slate-900 dark:text-white mb-4">创建用户</h2>
        
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              用户名 *
            </label>
            <input
              type="text"
              required
              value={formData.username}
              onChange={(e) => setFormData({...formData, username: e.target.value})}
              className="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-700 dark:text-white"
            />
          </div>
          
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              邮箱 *
            </label>
            <input
              type="email"
              required
              value={formData.email}
              onChange={(e) => setFormData({...formData, email: e.target.value})}
              className="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-700 dark:text-white"
            />
          </div>
          
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              密码 *
            </label>
            <input
              type="password"
              required
              minLength={8}
              value={formData.password}
              onChange={(e) => setFormData({...formData, password: e.target.value})}
              className="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-700 dark:text-white"
            />
          </div>
          
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              组织 *
            </label>
            <select
              required
              value={formData.org_id}
              onChange={(e) => setFormData({...formData, org_id: parseInt(e.target.value)})}
              className="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-700 dark:text-white"
            >
              {organizations.map(org => (
                <option key={org.id} value={org.id}>{org.name}</option>
              ))}
            </select>
          </div>
          
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              角色
            </label>
            <div className="space-y-2">
              {roles.map(role => (
                <label key={role.code} className="flex items-center">
                  <input
                    type="checkbox"
                    checked={formData.role_codes?.includes(role.code)}
                    onChange={(e) => {
                      const codes = formData.role_codes || [];
                      setFormData({
                        ...formData,
                        role_codes: e.target.checked
                          ? [...codes, role.code]
                          : codes.filter(c => c !== role.code)
                      });
                    }}
                    className="mr-2"
                  />
                  <span className="text-sm text-slate-900 dark:text-white">{role.name}</span>
                </label>
              ))}
            </div>
          </div>
          
          <div className="flex gap-3 pt-4">
            <button
              type="button"
              onClick={onClose}
              disabled={saving}
              className="flex-1 px-4 py-2 border border-slate-300 dark:border-slate-600 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700"
            >
              取消
            </button>
            <button
              type="submit"
              disabled={saving}
              className="flex-1 px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50"
            >
              {saving ? '创建中...' : '创建'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

// Edit User Modal
function EditUserModal({
  user,
  organizations,
  roles,
  onClose,
  onSuccess
}: {
  user: User;
  organizations: Organization[];
  roles: Role[];
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [formData, setFormData] = useState<UpdateUserRequest>({
    email: user.email,
    org_id: user.org_id || undefined,
    role_codes: user.roles.map(r => r.code),
    is_active: user.is_active,
    is_superuser: user.is_superuser
  });
  const [saving, setSaving] = useState(false);
  
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    
    try {
      await updateUser(user.id, formData);
      alert('用户更新成功');
      onSuccess();
    } catch (error: any) {
      alert(`更新失败: ${error.response?.data?.detail || error.message}`);
    } finally {
      setSaving(false);
    }
  };
  
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-md p-6">
        <h2 className="text-xl font-bold text-slate-900 dark:text-white mb-4">编辑用户</h2>
        <p className="text-sm text-slate-600 dark:text-slate-400 mb-4">用户名: {user.username}</p>
        
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              邮箱
            </label>
            <input
              type="email"
              value={formData.email || ''}
              onChange={(e) => setFormData({...formData, email: e.target.value})}
              className="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-700 dark:text-white"
            />
          </div>
          
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              组织
            </label>
            <select
              value={formData.org_id || ''}
              onChange={(e) => setFormData({...formData, org_id: e.target.value ? parseInt(e.target.value) : undefined})}
              className="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-700 dark:text-white"
            >
              <option value="">无</option>
              {organizations.map(org => (
                <option key={org.id} value={org.id}>{org.name}</option>
              ))}
            </select>
          </div>
          
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              角色
            </label>
            <div className="space-y-2">
              {roles.map(role => (
                <label key={role.code} className="flex items-center">
                  <input
                    type="checkbox"
                    checked={formData.role_codes?.includes(role.code)}
                    onChange={(e) => {
                      const codes = formData.role_codes || [];
                      setFormData({
                        ...formData,
                        role_codes: e.target.checked
                          ? [...codes, role.code]
                          : codes.filter(c => c !== role.code)
                      });
                    }}
                    className="mr-2"
                  />
                  <span className="text-sm text-slate-900 dark:text-white">{role.name}</span>
                </label>
              ))}
            </div>
          </div>
          
          <div className="flex items-center">
            <input
              type="checkbox"
              checked={formData.is_active}
              onChange={(e) => setFormData({...formData, is_active: e.target.checked})}
              className="mr-2"
            />
            <label className="text-sm text-slate-900 dark:text-white">用户已激活</label>
          </div>
          
          <div className="flex gap-3 pt-4">
            <button
              type="button"
              onClick={onClose}
              disabled={saving}
              className="flex-1 px-4 py-2 border border-slate-300 dark:border-slate-600 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700"
            >
              取消
            </button>
            <button
              type="submit"
              disabled={saving}
              className="flex-1 px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50"
            >
              {saving ? '更新中...' : '更新'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}










