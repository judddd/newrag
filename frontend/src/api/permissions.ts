/**
 * Document permissions API client
 */

import apiClient from './client';

export interface DocumentPermission {
  visibility: 'public' | 'organization' | 'private';
  shared_with_users: number[];
  shared_with_roles: string[];
}

export interface DocumentPermissionDetail {
  id: number;
  filename: string;
  visibility: string;
  owner: {
    id: number;
    username: string;
    email: string;
  } | null;
  organization: {
    id: number;
    name: string;
  } | null;
  shared_users: Array<{
    id: number;
    username: string;
    email: string;
  }>;
  shared_roles: Array<{
    code: string;
    name: string;
  }>;
}

/**
 * Get document permissions
 */
export async function getDocumentPermissions(docId: number): Promise<DocumentPermissionDetail> {
  const response = await apiClient.get(`/documents/${docId}/permissions`);
  return response.data;
}

/**
 * Update document permissions
 */
export async function updateDocumentPermissions(
  docId: number,
  permissions: DocumentPermission
): Promise<void> {
  await apiClient.put(`/documents/${docId}/permissions`, permissions);
}







