/**
 * Admin API client for user and organization management
 */

import apiClient from './client';

// ============================================================================
// Types
// ============================================================================

export interface User {
  id: number;
  username: string;
  email: string;
  org_id: number | null;
  org_name?: string;
  roles: Array<{ code: string; name: string }>;
  is_active: boolean;
  is_superuser: boolean;
  created_at: string;
  last_login: string | null;
}

export interface UserDetail extends User {
  permissions: string[];
}

export interface CreateUserRequest {
  username: string;
  email: string;
  password: string;
  org_id: number;
  role_codes?: string[];
  is_active?: boolean;
  is_superuser?: boolean;
}

export interface UpdateUserRequest {
  email?: string;
  org_id?: number;
  role_codes?: string[];
  is_active?: boolean;
  is_superuser?: boolean;
}

export interface ResetPasswordRequest {
  new_password: string;
}

export interface Organization {
  id: number;
  name: string;
  description: string | null;
  member_count: number;
  document_count: number;
  created_at: string;
}

export interface OrganizationDetail {
  id: number;
  name: string;
  description: string | null;
  members: User[];
  created_at: string;
}

export interface CreateOrganizationRequest {
  name: string;
  description?: string;
}

export interface UpdateOrganizationRequest {
  name: string;
  description?: string;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  per_page: number;
  total_pages: number;
}

export interface Role {
  id: number;
  name: string;
  code: string;
  description: string | null;
  is_system: boolean;
}

// ============================================================================
// User Management
// ============================================================================

export async function listUsers(params: {
  page?: number;
  per_page?: number;
  search?: string;
  org_id?: number;
  role_code?: string;
  is_active?: boolean;
}): Promise<PaginatedResponse<User>> {
  const response = await apiClient.get('/admin/users', { params });
  return response.data;
}

export async function getUser(userId: number): Promise<UserDetail> {
  const response = await apiClient.get(`/admin/users/${userId}`);
  return response.data;
}

export async function createUser(data: CreateUserRequest): Promise<UserDetail> {
  const response = await apiClient.post('/admin/users', data);
  return response.data;
}

export async function updateUser(userId: number, data: UpdateUserRequest): Promise<UserDetail> {
  const response = await apiClient.put(`/admin/users/${userId}`, data);
  return response.data;
}

export async function deleteUser(userId: number): Promise<void> {
  await apiClient.delete(`/admin/users/${userId}`);
}

export async function resetUserPassword(userId: number, data: ResetPasswordRequest): Promise<void> {
  await apiClient.post(`/admin/users/${userId}/reset-password`, data);
}

// ============================================================================
// Organization Management
// ============================================================================

export async function listOrganizations(): Promise<Organization[]> {
  const response = await apiClient.get('/admin/organizations');
  return response.data;
}

export async function getOrganization(orgId: number): Promise<OrganizationDetail> {
  const response = await apiClient.get(`/admin/organizations/${orgId}`);
  return response.data;
}

export async function createOrganization(data: CreateOrganizationRequest): Promise<Organization> {
  const response = await apiClient.post('/admin/organizations', data);
  return response.data;
}

export async function updateOrganization(
  orgId: number,
  data: UpdateOrganizationRequest
): Promise<Organization> {
  const response = await apiClient.put(`/admin/organizations/${orgId}`, data);
  return response.data;
}

export async function deleteOrganization(orgId: number): Promise<void> {
  await apiClient.delete(`/admin/organizations/${orgId}`);
}

// ============================================================================
// Role Management
// ============================================================================

export async function listRoles(): Promise<Role[]> {
  const response = await apiClient.get('/admin/roles');
  return response.data;
}





