/**
 * Admin Organizations Management Page
 * 
 * Allows administrators to manage organizations
 */

import { useState, useEffect } from 'react';
import { Building2, Plus, Edit, Trash2, Users } from 'lucide-react';
import {
  listOrganizations,
  getOrganization,
  createOrganization,
  updateOrganization,
  deleteOrganization,
  type Organization,
  type OrganizationDetail,
  type CreateOrganizationRequest,
  type UpdateOrganizationRequest
} from '../api/admin';

export default function AdminOrganizationsPage() {
  const [organizations, setOrganizations] = useState<Organization[]>([]);
  const [loading, setLoading] = useState(false);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showEditModal, setShowEditModal] = useState(false);
  const [showDetailModal, setShowDetailModal] = useState(false);
  const [selectedOrg, setSelectedOrg] = useState<Organization | null>(null);
  const [selectedOrgDetail, setSelectedOrgDetail] = useState<OrganizationDetail | null>(null);
  
  useEffect(() => {
    loadOrganizations();
  }, []);
  
  const loadOrganizations = async () => {
    setLoading(true);
    try {
      const data = await listOrganizations();
      setOrganizations(data);
    } catch (error: any) {
      console.error('Failed to load organizations:', error);
      alert(`加载组织失败: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };
  
  const handleViewDetails = async (org: Organization) => {
    try {
      const detail = await getOrganization(org.id);
      setSelectedOrgDetail(detail);
      setShowDetailModal(true);
    } catch (error: any) {
      alert(`加载组织详情失败: ${error.message}`);
    }
  };
  
  const handleEditOrg = (org: Organization) => {
    setSelectedOrg(org);
    setShowEditModal(true);
  };
  
  const handleDeleteOrg = async (org: Organization) => {
    if (!confirm(`确定要删除组织 "${org.name}"?\n\n注意：只能删除没有用户和文档的组织。`)) {
      return;
    }
    
    try {
      await deleteOrganization(org.id);
      alert('组织已删除');
      loadOrganizations();
    } catch (error: any) {
      alert(`删除失败: ${error.response?.data?.detail || error.message}`);
    }
  };
  
  return (
    <div className="min-h-screen bg-slate-50 dark:bg-slate-900 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-slate-900 dark:text-white flex items-center gap-3">
              <Building2 size={32} className="text-indigo-600" />
              组织管理
            </h1>
            <p className="text-slate-600 dark:text-slate-400 mt-1">
              管理系统组织和成员
            </p>
          </div>
          <button
            onClick={() => setShowCreateModal(true)}
            className="flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 transition-colors"
          >
            <Plus size={20} />
            创建组织
          </button>
        </div>
        
        {/* Organizations Grid */}
        {loading ? (
          <div className="text-center py-12">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-600 mx-auto"></div>
            <p className="mt-4 text-slate-600 dark:text-slate-400">加载中...</p>
          </div>
        ) : organizations.length === 0 ? (
          <div className="bg-white dark:bg-slate-800 rounded-lg shadow-md p-12 text-center">
            <Building2 size={48} className="mx-auto text-slate-400 mb-4" />
            <p className="text-slate-600 dark:text-slate-400">暂无组织</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {organizations.map(org => (
              <div
                key={org.id}
                className="bg-white dark:bg-slate-800 rounded-lg shadow-md hover:shadow-lg transition-shadow overflow-hidden"
              >
                <div className="p-6">
                  <div className="flex items-start justify-between mb-4">
                    <div className="flex-1">
                      <h3 className="text-lg font-semibold text-slate-900 dark:text-white mb-1">
                        {org.name}
                      </h3>
                      {org.description && (
                        <p className="text-sm text-slate-600 dark:text-slate-400 line-clamp-2">
                          {org.description}
                        </p>
                      )}
                    </div>
                  </div>
                  
                  <div className="grid grid-cols-2 gap-4 mb-4">
                    <div className="bg-indigo-50 dark:bg-indigo-900/20 rounded-lg p-3">
                      <div className="flex items-center text-indigo-600 dark:text-indigo-400 mb-1">
                        <Users size={16} className="mr-1" />
                        <span className="text-xs font-medium">成员</span>
                      </div>
                      <div className="text-2xl font-bold text-slate-900 dark:text-white">
                        {org.member_count}
                      </div>
                    </div>
                    
                    <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-3">
                      <div className="flex items-center text-green-600 dark:text-green-400 mb-1">
                        <Building2 size={16} className="mr-1" />
                        <span className="text-xs font-medium">文档</span>
                      </div>
                      <div className="text-2xl font-bold text-slate-900 dark:text-white">
                        {org.document_count}
                      </div>
                    </div>
                  </div>
                  
                  <div className="flex gap-2">
                    <button
                      onClick={() => handleViewDetails(org)}
                      className="flex-1 px-3 py-2 text-sm bg-indigo-50 dark:bg-indigo-900/20 text-indigo-600 dark:text-indigo-400 rounded-lg hover:bg-indigo-100 dark:hover:bg-indigo-900/40 transition-colors"
                    >
                      查看详情
                    </button>
                    <button
                      onClick={() => handleEditOrg(org)}
                      className="p-2 text-slate-600 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-lg transition-colors"
                      title="编辑"
                    >
                      <Edit size={18} />
                    </button>
                    <button
                      onClick={() => handleDeleteOrg(org)}
                      className="p-2 text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-lg transition-colors"
                      title="删除"
                    >
                      <Trash2 size={18} />
                    </button>
                  </div>
                </div>
                
                <div className="bg-slate-50 dark:bg-slate-700/50 px-6 py-3 text-xs text-slate-500 dark:text-slate-400">
                  创建于: {new Date(org.created_at).toLocaleDateString('zh-CN')}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
      
      {/* Modals */}
      {showCreateModal && (
        <CreateOrganizationModal
          onClose={() => setShowCreateModal(false)}
          onSuccess={() => {
            setShowCreateModal(false);
            loadOrganizations();
          }}
        />
      )}
      
      {showEditModal && selectedOrg && (
        <EditOrganizationModal
          org={selectedOrg}
          onClose={() => {
            setShowEditModal(false);
            setSelectedOrg(null);
          }}
          onSuccess={() => {
            setShowEditModal(false);
            setSelectedOrg(null);
            loadOrganizations();
          }}
        />
      )}
      
      {showDetailModal && selectedOrgDetail && (
        <OrganizationDetailModal
          org={selectedOrgDetail}
          onClose={() => {
            setShowDetailModal(false);
            setSelectedOrgDetail(null);
          }}
        />
      )}
    </div>
  );
}

// Create Organization Modal
function CreateOrganizationModal({
  onClose,
  onSuccess
}: {
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [formData, setFormData] = useState<CreateOrganizationRequest>({
    name: '',
    description: ''
  });
  const [saving, setSaving] = useState(false);
  
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    
    try {
      await createOrganization(formData);
      alert('组织创建成功');
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
        <h2 className="text-xl font-bold text-slate-900 dark:text-white mb-4">创建组织</h2>
        
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              组织名称 *
            </label>
            <input
              type="text"
              required
              value={formData.name}
              onChange={(e) => setFormData({...formData, name: e.target.value})}
              className="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-700 dark:text-white"
            />
          </div>
          
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              描述
            </label>
            <textarea
              value={formData.description || ''}
              onChange={(e) => setFormData({...formData, description: e.target.value})}
              rows={3}
              className="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-700 dark:text-white"
            />
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

// Edit Organization Modal
function EditOrganizationModal({
  org,
  onClose,
  onSuccess
}: {
  org: Organization;
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [formData, setFormData] = useState<UpdateOrganizationRequest>({
    name: org.name,
    description: org.description || ''
  });
  const [saving, setSaving] = useState(false);
  
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    
    try {
      await updateOrganization(org.id, formData);
      alert('组织更新成功');
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
        <h2 className="text-xl font-bold text-slate-900 dark:text-white mb-4">编辑组织</h2>
        
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              组织名称 *
            </label>
            <input
              type="text"
              required
              value={formData.name}
              onChange={(e) => setFormData({...formData, name: e.target.value})}
              className="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-700 dark:text-white"
            />
          </div>
          
          <div>
            <label className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
              描述
            </label>
            <textarea
              value={formData.description || ''}
              onChange={(e) => setFormData({...formData, description: e.target.value})}
              rows={3}
              className="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-700 dark:text-white"
            />
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

// Organization Detail Modal
function OrganizationDetailModal({
  org,
  onClose
}: {
  org: OrganizationDetail;
  onClose: () => void;
}) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-hidden flex flex-col">
        <div className="flex items-center justify-between p-6 border-b border-slate-200 dark:border-slate-700">
          <div>
            <h2 className="text-xl font-bold text-slate-900 dark:text-white">组织详情</h2>
            <p className="text-sm text-slate-600 dark:text-slate-400 mt-1">{org.name}</p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-lg transition-colors"
          >
            <span className="text-2xl">&times;</span>
          </button>
        </div>
        
        <div className="flex-1 overflow-y-auto p-6">
          {org.description && (
            <div className="mb-6">
              <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">描述</h3>
              <p className="text-slate-600 dark:text-slate-400">{org.description}</p>
            </div>
          )}
          
          <div>
            <h3 className="text-sm font-medium text-slate-700 dark:text-slate-300 mb-3">
              成员列表 ({org.members.length})
            </h3>
            
            {org.members.length === 0 ? (
              <p className="text-center py-8 text-slate-500 dark:text-slate-400">暂无成员</p>
            ) : (
              <div className="space-y-2">
                {org.members.map(member => (
                  <div
                    key={member.id}
                    className="flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg"
                  >
                    <div>
                      <div className="font-medium text-slate-900 dark:text-white">
                        {member.username}
                        {member.is_superuser && (
                          <span className="ml-2 px-2 py-0.5 text-xs bg-red-100 text-red-800 rounded-full">
                            管理员
                          </span>
                        )}
                      </div>
                      <div className="text-sm text-slate-600 dark:text-slate-400">{member.email}</div>
                    </div>
                    <div className="flex flex-wrap gap-1">
                      {member.roles.map(role => (
                        <span
                          key={role.code}
                          className="px-2 py-1 text-xs bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-200 rounded"
                        >
                          {role.name}
                        </span>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
        
        <div className="p-6 border-t border-slate-200 dark:border-slate-700">
          <button
            onClick={onClose}
            className="w-full px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 transition-colors"
          >
            关闭
          </button>
        </div>
      </div>
    </div>
  );
}









