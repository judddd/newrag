/**
 * Authentication Token Management Utilities
 */

const ACCESS_TOKEN_KEY = 'access_token';
const REFRESH_TOKEN_KEY = 'refresh_token';
const TOKEN_EXPIRY_KEY = 'token_expiry';

/**
 * Store authentication tokens in localStorage
 */
export function setTokens(accessToken: string, refreshToken: string, expiresIn: number): void {
  localStorage.setItem(ACCESS_TOKEN_KEY, accessToken);
  localStorage.setItem(REFRESH_TOKEN_KEY, refreshToken);
  
  // Calculate expiry timestamp (expiresIn is in seconds)
  const expiryTime = Date.now() + (expiresIn * 1000);
  localStorage.setItem(TOKEN_EXPIRY_KEY, expiryTime.toString());
}

/**
 * Get access token from localStorage
 */
export function getAccessToken(): string | null {
  return localStorage.getItem(ACCESS_TOKEN_KEY);
}

/**
 * Get refresh token from localStorage
 */
export function getRefreshToken(): string | null {
  return localStorage.getItem(REFRESH_TOKEN_KEY);
}

/**
 * Check if token is expired or will expire soon (within 5 minutes)
 */
export function isTokenExpiringSoon(): boolean {
  const expiryStr = localStorage.getItem(TOKEN_EXPIRY_KEY);
  if (!expiryStr) return true;
  
  const expiry = parseInt(expiryStr);
  const now = Date.now();
  const fiveMinutes = 5 * 60 * 1000;
  
  return (expiry - now) < fiveMinutes;
}

/**
 * Check if user is authenticated (has valid token)
 */
export function isAuthenticated(): boolean {
  const token = getAccessToken();
  return token !== null && !isTokenExpiringSoon();
}

/**
 * Clear all authentication tokens
 */
export function clearTokens(): void {
  localStorage.removeItem(ACCESS_TOKEN_KEY);
  localStorage.removeItem(REFRESH_TOKEN_KEY);
  localStorage.removeItem(TOKEN_EXPIRY_KEY);
}

/**
 * Parse JWT token to extract payload (without validation)
 * WARNING: This does not validate the token signature!
 */
export function parseJwtPayload(token: string): any {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) {
      throw new Error('Invalid JWT format');
    }
    
    const payload = parts[1];
    const decoded = atob(payload.replace(/-/g, '+').replace(/_/g, '/'));
    return JSON.parse(decoded);
  } catch (error) {
    console.error('Failed to parse JWT:', error);
    return null;
  }
}

/**
 * Get user info from stored access token
 */
export function getUserFromToken(): {
  id: number;
  username: string;
  is_superuser: boolean;
  org_id?: number;
} | null {
  const token = getAccessToken();
  if (!token) return null;
  
  const payload = parseJwtPayload(token);
  if (!payload) return null;
  
  return {
    id: parseInt(payload.sub),
    username: payload.username,
    is_superuser: payload.is_superuser || false,
    org_id: payload.org_id,
  };
}







