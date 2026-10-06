import * as SecureStore from 'expo-secure-store';
import { CityOption, Shelf, ShelfListQuery, StationOption, User, WaterOrder } from './types';

export const API_URL =
  process.env.EXPO_PUBLIC_API_URL || 'https://iot.stonedrumpoem.cn/api';

export class AuthExpiredError extends Error {
  constructor(message = '登录已过期，请重新登录') {
    super(message);
    this.name = 'AuthExpiredError';
  }
}

const AUTO_LOGIN_KEY = 'auto_login';
const SAVED_USERNAME_KEY = 'saved_username';
const SAVED_PASSWORD_KEY = 'saved_password';

export type SavedLogin = {
  username: string;
  password: string;
};

async function clearSessionLocal(): Promise<void> {
  await SecureStore.deleteItemAsync('token');
  await SecureStore.deleteItemAsync('user');
}

async function deleteIfPresent(key: string): Promise<void> {
  await SecureStore.deleteItemAsync(key).catch(() => undefined);
}

export async function getSavedLogin(): Promise<SavedLogin | null> {
  const enabled = await SecureStore.getItemAsync(AUTO_LOGIN_KEY);
  if (enabled !== '1') return null;
  const username = await SecureStore.getItemAsync(SAVED_USERNAME_KEY);
  const password = await SecureStore.getItemAsync(SAVED_PASSWORD_KEY);
  if (!username || !password) return null;
  return { username, password };
}

export async function clearAutoLogin(): Promise<void> {
  await Promise.all([
    deleteIfPresent(AUTO_LOGIN_KEY),
    deleteIfPresent(SAVED_USERNAME_KEY),
    deleteIfPresent(SAVED_PASSWORD_KEY),
  ]);
}

async function saveAutoLogin(username: string, password: string): Promise<void> {
  await SecureStore.setItemAsync(AUTO_LOGIN_KEY, '1');
  await SecureStore.setItemAsync(SAVED_USERNAME_KEY, username);
  await SecureStore.setItemAsync(SAVED_PASSWORD_KEY, password);
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

export async function login(
  username: string,
  password: string,
  autoLogin = false,
): Promise<User> {
  const result = await request<{ access_token: string; user: User }>('/auth/login', {
    method: 'POST',
    body: JSON.stringify({ username, password }),
  });
  await SecureStore.setItemAsync('token', result.access_token);
  await SecureStore.setItemAsync('user', JSON.stringify(result.user));
  if (autoLogin) {
    await saveAutoLogin(username, password);
  } else {
    await clearAutoLogin();
  }
  return result.user;
}

/** 用已保存的账号密码重新登录。失败时保留凭据，交给登录页重试。 */
export async function tryAutoLogin(): Promise<User | null> {
  const saved = await getSavedLogin();
  if (!saved) return null;
  try {
    return await login(saved.username, saved.password, true);
  } catch {
    return null;
  }
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

export type OrderListFilters = {
  paymentStatus?: 'unpaid' | 'paid' | '';
  result?: 'delivered' | 'cancelled' | '';
  search?: string;
};

export async function getOrders(
  status: 'pending' | 'delivering' | 'history',
  page: number = 1,
  pageSize: number = 20,
  filters: OrderListFilters = {},
) {
  const skip = Math.max(0, (page - 1) * pageSize);
  const params = new URLSearchParams({
    status,
    skip: String(skip),
    limit: String(pageSize),
  });
  if (filters.paymentStatus === 'unpaid' || filters.paymentStatus === 'paid') {
    params.set('payment_status', filters.paymentStatus);
  }
  if (filters.result === 'delivered' || filters.result === 'cancelled') {
    params.set('result', filters.result);
  }
  const keyword = filters.search?.trim();
  if (keyword) {
    params.set('search', keyword);
  }
  return request<{ orders: WaterOrder[]; total_count: number }>(
    `/water-orders?${params.toString()}`,
  );
}

export async function startDelivery(orderId: number, paymentStatus?: 'unpaid' | 'paid') {
  return request<WaterOrder>(`/water-orders/${orderId}/start-delivery`, {
    method: 'POST',
    body: JSON.stringify({
      ...(paymentStatus ? { payment_status: paymentStatus } : {}),
    }),
  });
}

export async function deliverOrder(
  orderId: number,
  deliveredQuantity: number,
  paymentStatus?: 'unpaid' | 'paid',
) {
  return request<WaterOrder>(`/water-orders/${orderId}/deliver`, {
    method: 'POST',
    body: JSON.stringify({
      delivered_quantity: deliveredQuantity,
      ...(paymentStatus ? { payment_status: paymentStatus } : {}),
    }),
  });
}

export async function updateOrderPayment(orderId: number, paymentStatus: 'unpaid' | 'paid') {
  return request<WaterOrder>(`/water-orders/${orderId}/payment`, {
    method: 'PATCH',
    body: JSON.stringify({ payment_status: paymentStatus }),
  });
}

export async function updateOrderSettings(
  orderId: number,
  scheduledAt: string | null,
  remark: string,
) {
  return request<WaterOrder>(`/water-orders/${orderId}/settings`, {
    method: 'PATCH',
    body: JSON.stringify({
      scheduled_at: scheduledAt,
      remark,
    }),
  });
}

export async function cancelOrder(orderId: number) {
  return request<WaterOrder>(`/water-orders/${orderId}/cancel`, {
    method: 'POST',
  });
}

export async function getShelves(query: ShelfListQuery = {}) {
  const page = query.page ?? 1;
  const pageSize = query.pageSize ?? 10;
  const skip = Math.max(0, (page - 1) * pageSize);
  const params = new URLSearchParams({
    skip: String(skip),
    limit: String(pageSize),
    current_quantity_min: query.minQuantity?.trim() ? query.minQuantity.trim() : '-999',
  });
  const maxQuantity = query.maxQuantity?.trim();
  if (maxQuantity) params.set('current_quantity_max', maxQuantity);
  const keyword = query.search?.trim();
  if (keyword) params.set('search', keyword);
  if (query.cityId) params.set('city_id', String(query.cityId));
  if (query.stationId) params.set('station_id', String(query.stationId));
  if (query.deviceType) params.set('device_type', query.deviceType);
  if (query.onlineStatus === '0' || query.onlineStatus === '1') params.set('online_status', query.onlineStatus);
  if (query.deliveryStatus === '1' || query.deliveryStatus === '2') params.set('delivery_status', query.deliveryStatus);
  if (query.paymentMethod) params.set('payment_method', query.paymentMethod);
  if (query.lowStock) params.set('low_stock_alert', 'true');
  if (query.lowVoltage) params.set('low_voltage_alert', 'true');
  if (query.simExpiry) params.set('sim_expiry_alert', 'true');
  if (query.lowSignal) params.set('low_signal_alert', 'true');
  return request<Shelf[]>(`/shelves?${params.toString()}`);
}

export async function getCities() {
  return request<CityOption[]>('/cities?skip=0&limit=100');
}

export async function getStations(cityId?: number | null) {
  const params = new URLSearchParams({ skip: '0', limit: '100' });
  if (cityId) params.set('city_id', String(cityId));
  return request<StationOption[]>(`/stations?${params.toString()}`);
}

export type ShelfWritePayload = {
  iccid: string;
  wechat: string;
  phone: string;
  address: string;
  total_quantity: number;
  product_name: string;
  order_quantity: number;
  current_quantity: number;
  warning_quantity: number;
  sim_card_number: string;
  sim_card_expiry: string;
  station_id: number;
  device_type: 'shelf' | 'tea_bar';
  payment_method?: string;
  remark?: string;
  voltage?: number;
  signal_strength?: number;
  version?: string;
  longitude?: number | null;
  latitude?: number | null;
  created_by: string;
  updated_by: string;
};

export async function createShelf(payload: ShelfWritePayload) {
  return request<Shelf>('/shelves', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function updateShelf(id: number, payload: Partial<ShelfWritePayload>) {
  return request<Shelf>(`/shelves/${id}`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  });
}

export async function deleteShelf(id: number) {
  return request<void>(`/shelves/${id}`, { method: 'DELETE' });
}

export async function shipShelf(id: number) {
  return request<void>(`/shelves/${id}/deliver`, { method: 'POST' });
}

export async function getShelfQuantityLogs(shelfId: number) {
  return request<Array<{ id: number; log_time?: string | null; current_quantity: number }>>(
    `/shelf-logs?shelf_id=${shelfId}&skip=0&limit=20`,
  );
}

export async function getShelfShippingLogs(shelfId: number) {
  return request<{ logs: Array<{ id: number; ship_time?: string | null }>; total_count: number }>(
    `/shelf-shipping-logs/shelf/${shelfId}?skip=0&limit=20`,
  );
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
