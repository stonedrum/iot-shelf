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
  station_name?: string;
  city_name?: string;
}
