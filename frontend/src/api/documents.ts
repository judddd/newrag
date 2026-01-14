import apiClient from './client';

export interface Document {
  id: number;
  filename: string;
  file_path: string;
  file_type: string;
  file_size: number;
  checksum: string;
  status: string;
  total_pages?: number;
  processed_pages?: number;
  progress_percentage?: number;
  progress_message?: string;
  created_at: string;
  updated_at: string;
  category?: string;
  tags?: string[];
  // Version control fields
  document_group_id?: string;
  version?: number;
  is_latest?: boolean;
  version_count?: number;
}

export interface DocumentVersion {
  id: number;
  version: number;
  file_size: number;
  status: string;
  uploaded_at: string;
  uploaded_by?: {
    id: number;
    username: string;
    email: string;
  };
  version_note?: string;
  is_latest: boolean;
  checksum: string;
}

export interface VersionHistoryResponse {
  document_group_id: string;
  filename: string;
  versions: DocumentVersion[];
  total_versions: number;
}

export interface DocumentListResponse {
  documents: Document[];
  total: number;
}

export const documentAPI = {
  // 获取文档列表
  list: async (params?: { 
    limit?: number; 
    offset?: number; 
    status?: string; 
    organization_id?: number;
    search?: string;
    sort_by?: string;
    sort_order?: string;
  }) => {
    const response = await apiClient.get<DocumentListResponse>('/documents', { params });
    return response.data;
  },

  // 获取文档进度
  getProgress: async (docId: number, includeChildren: boolean = false) => {
    const response = await apiClient.get(`/documents/${docId}/progress`, {
      params: { include_children: includeChildren }
    });
    return response.data;
  },

  // 上传文件
  upload: async (file: File, metadata?: {
    category?: string;
    tags?: string;
    author?: string;
    description?: string;
    ocr_engine?: string;
    processing_mode?: string;
    organization_id?: number;
    visibility?: string;
  }) => {
    const formData = new FormData();
    formData.append('file', file);
    
    if (metadata) {
      Object.entries(metadata).forEach(([key, value]) => {
        if (value !== null && value !== undefined) {
          formData.append(key, String(value));
        }
      });
    }

    const response = await apiClient.post('/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  },

  // 批量上传
  uploadBatch: async (files: File[], metadata?: {
    category?: string;
    tags?: string;
    author?: string;
    description?: string;
    ocr_engine?: string;
    processing_mode?: string;
    organization_id?: number;
    visibility?: string;
  }) => {
    const formData = new FormData();
    files.forEach(file => formData.append('files', file));
    
    if (metadata) {
      Object.entries(metadata).forEach(([key, value]) => {
        if (value !== null && value !== undefined) {
          formData.append(key, String(value));
        }
      });
    }

    const response = await apiClient.post('/upload_batch', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  },

  // 删除文档
  delete: async (docId: number) => {
    const response = await apiClient.delete(`/documents/${docId}`);
    return response.data;
  },

  // ===== Version Control APIs =====
  
  // 获取文档版本历史
  getVersionHistory: async (documentGroupId: string) => {
    const response = await apiClient.get<VersionHistoryResponse>(`/documents/${documentGroupId}/versions`);
    return response.data;
  },

  // 获取特定版本
  getSpecificVersion: async (documentGroupId: string, versionNumber: number) => {
    const response = await apiClient.get<Document>(`/documents/${documentGroupId}/versions/${versionNumber}`);
    return response.data;
  },

  // 恢复版本
  restoreVersion: async (documentGroupId: string, versionNumber: number) => {
    const response = await apiClient.post(`/documents/${documentGroupId}/versions/${versionNumber}/restore`);
    return response.data;
  },

  // 删除版本
  deleteVersion: async (documentGroupId: string, versionNumber: number, hardDelete: boolean = false) => {
    const response = await apiClient.delete(`/documents/${documentGroupId}/versions/${versionNumber}`, {
      params: { hard_delete: hardDelete }
    });
    return response.data;
  },

  // 更新文档元数据
  updateMetadata: async (documentGroupId: string, metadata: {
    category?: string;
    tags?: string;
    author?: string;
    description?: string;
  }) => {
    const params = new URLSearchParams();
    if (metadata.category) params.append('category', metadata.category);
    if (metadata.tags) params.append('tags', metadata.tags);
    if (metadata.author) params.append('author', metadata.author);
    if (metadata.description) params.append('description', metadata.description);
    
    const response = await apiClient.put(`/documents/${documentGroupId}/metadata?${params.toString()}`);
    return response.data;
  },

  // 删除所有文档
  deleteAll: async () => {
    const response = await apiClient.delete('/documents');
    return response.data;
  },

  // 清理 MinIO 数据
  cleanupMinIO: async (docId: number) => {
    const response = await apiClient.post(`/documents/${docId}/cleanup-minio`);
    return response.data;
  },

  // 获取可用机构列表（用于过滤）
  getOrganizations: async () => {
    const response = await apiClient.get<Array<{ id: number; name: string; description: string }>>('/auth/organizations');
    return response.data;
  },
};












