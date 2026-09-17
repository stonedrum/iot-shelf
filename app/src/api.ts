import * as SecureStore from 'expo-secure-store';
import { Shelf, User, WaterOrder } from './types';

export const API_URL =
  process.env.EXPO_PUBLIC_API_URL || 'https://iot.stonedrumpoem.cn/api';

export class AuthExpiredError extends Error {
  constructor(message = '登录已过期，请重新登录') {
    super(message);
    this.name = 'AuthExpiredError';
  }
}

async function clearSessionLocal(): Promise<void> {
  await SecureStore.deleteItemAsync('token');
  await SecureStore.deleteItemAsync('user');
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = await SecureStore.getItemAsync('token');
  const response = await fetch(`${API_URL}${path}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...init.headers,
    },
  });
  if (response.status === 401) {
    await clearSessionLocal();
    throw new AuthExpiredError();
  }
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail || `请求失败 (${response.status})`);
  }
  if (response.status === 204) return undefined as T;
  return response.json();
}

export async function login(username: string, password: string): Promise<User> {
  const result = await request<{ access_token: string; user: User }>('/auth/login', {
    method: 'POST',
    body: JSON.stringify({ username, password }),
  });
  await SecureStore.setItemAsync('token', result.access_token);
  await SecureStore.setItemAsync('user', JSON.stringify(result.user));
  return result.user;
}

export async function logout(): Promise<void> {
  const pushCid = await SecureStore.getItemAsync('getui_cid');
  if (pushCid) {
    await request<void>(`/push-tokens?token=${encodeURIComponent(pushCid)}`, {
      method: 'DELETE',
    }).catch(error => {
      if (!(error instanceof AuthExpiredError)) {
        console.warn('注销个推 CID 失败', error);
      }
    });
  }
  await clearSessionLocal();
  await SecureStore.deleteItemAsync('getui_cid');
}

/** 校验本地 token 是否仍有效；过期则清空并返回 null。 */
export async function restoreUser(): Promise<User | null> {
  const token = await SecureStore.getItemAsync('token');
  const cached = await SecureStore.getItemAsync('user');
  if (!token || !cached) {
    await clearSessionLocal();
    return null;
  }

  try {
    const me = await request<User>('/users/me');
    await SecureStore.setItemAsync('user', JSON.stringify(me));
    return me;
  } catch (error) {
    if (error instanceof AuthExpiredError) {
      return null;
    }
    // 网络异常时先沿用本地缓存，避免误踢出登录
    try {
      return JSON.parse(cached) as User;
    } catch {
      await clearSessionLocal();
      return null;
    }
  }
}

export async function getOrders(
  status: 'pending' | 'history',
  page: number = 1,
  pageSize: number = 20,
) {
  const skip = Math.max(0, (page - 1) * pageSize);
  return request<{ orders: WaterOrder[]; total_count: number }>(
    `/water-orders?status=${status}&skip=${skip}&limit=${pageSize}`,
  );
}

export async function deliverOrder(orderId: number, deliveredQuantity: number) {
  return request<WaterOrder>(`/water-orders/${orderId}/deliver`, {
    method: 'POST',
    body: JSON.stringify({ delivered_quantity: deliveredQuantity }),
  });
}

export async function getShelves(
  search?: string,
  page: number = 1,
  pageSize: number = 10,
) {
  const skip = Math.max(0, (page - 1) * pageSize);
  const params = new URLSearchParams({
    current_quantity_min: '-999',
    skip: String(skip),
    limit: String(pageSize),
  });
  const keyword = search?.trim();
  if (keyword) {
    params.set('search', keyword);
  }
  return request<Shelf[]>(`/shelves?${params.toString()}`);
}

export async function getActiveOrder(shelfId: number) {
  return request<WaterOrder | null>(`/water-orders/shelf/${shelfId}/active`);
}

export async function createOrder(shelfId: number) {
  return request<WaterOrder>('/water-orders', {
    method: 'POST',
    body: JSON.stringify({ shelf_id: shelfId }),
  });
}

export async function registerPushToken(token: string, platform: string) {
  await request<void>('/push-tokens', {
    method: 'POST',
    body: JSON.stringify({ token, platform }),
  });
  await SecureStore.setItemAsync('getui_cid', token);
}
