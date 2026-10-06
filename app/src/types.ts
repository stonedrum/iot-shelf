export interface User {
  id: number;
  username: string;
  role: 'admin' | 'general';
}

export type WaterOrderStatus = 'pending' | 'delivering' | 'delivered' | 'cancelled';
export type WaterOrderPaymentStatus = 'unpaid' | 'paid';

export interface WaterOrder {
  id: number;
  order_no: string;
  shelf_id: number;
  station_id: number;
  source: 'auto' | 'manual';
  status: WaterOrderStatus;
  payment_status: WaterOrderPaymentStatus;
  scheduled_at?: string | null;
  remark?: string;
  requested_quantity: number;
  delivered_quantity: number | null;
  trigger_quantity: number;
  stock_before_delivery: number | null;
  stock_after_delivery: number | null;
  delivered_at: string | null;
  created_at: string;
  shelf_iccid: string;
  product_name: string;
  address: string;
  shelf_phone: string;
  shelf_wechat: string;
  voltage: number | null;
  current_quantity: number;
  warning_quantity: number;
  station_name: string;
}

export interface Shelf {
  id: number;
  iccid: string;
  product_name: string;
  address: string;
  phone?: string;
  wechat?: string;
  current_quantity: number;
  warning_quantity: number;
  total_quantity: number;
  order_quantity: number;
  station_id?: number;
  station_name?: string;
  city_name?: string;
  device_type?: 'shelf' | 'tea_bar' | string;
  voltage?: number | null;
  online_status?: number | null;
  delivery_status?: number | null;
  payment_method?: string | null;
  signal_strength?: number | null;
  sim_card_number?: string | null;
  sim_card_expiry?: string | null;
  push_time?: string | null;
  remark?: string | null;
  version?: string | null;
  longitude?: number | null;
  latitude?: number | null;
}

export interface CityOption {
  id: number;
  city_name: string;
}

export interface StationOption {
  id: number;
  station_name: string;
  city_id: number;
}

export type ShelfListQuery = {
  search?: string;
  cityId?: number | null;
  stationId?: number | null;
  deviceType?: '' | 'shelf' | 'tea_bar';
  onlineStatus?: '' | '0' | '1';
  deliveryStatus?: '' | '1' | '2';
  paymentMethod?: string;
  minQuantity?: string;
  maxQuantity?: string;
  lowStock?: boolean;
  lowVoltage?: boolean;
  simExpiry?: boolean;
  lowSignal?: boolean;
  page?: number;
  pageSize?: number;
};
