import { StatusBar } from 'expo-status-bar';
import DateTimePicker, { DateTimePickerEvent } from '@react-native-community/datetimepicker';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  ActivityIndicator,
  Alert,
  AppState,
  FlatList,
  Keyboard,
  Modal,
  Linking,
  Platform,
  Pressable,
  RefreshControl,
  SafeAreaView,
  StatusBar as RNStatusBar,
  StyleSheet,
  Text,
  TextInput,
  TextStyle,
  View,
} from 'react-native';
import {
  AuthExpiredError,
  cancelOrder,
  createOrder,
  deliverOrder,
  getActiveOrder,
  getOrders,
  clearAutoLogin,
  getSavedLogin,
  login,
  logout,
  restoreUser,
  tryAutoLogin,
  startDelivery,
  updateOrderPayment,
  updateOrderSettings,
} from './src/api';
import { setupPushNotifications } from './src/notifications';
import { ShelfPane } from './src/ShelfPane';
import { Shelf, User, WaterOrder, WaterOrderPaymentStatus } from './src/types';

type Tab = 'pending' | 'delivering' | 'shelves' | 'history' | 'more';
type HistoryPaymentFilter = '' | 'unpaid' | 'paid';
type HistoryResultFilter = '' | 'delivered' | 'cancelled';
const PAGE_SIZE = 20;
const APP_VERSION = '1.0.0';
const ACCENT = '#2563eb';
const DANGER = '#e54d42';
const LOW_VOLTAGE_THRESHOLD = 36.1;
const TOP_INSET = Platform.OS === 'android' ? (RNStatusBar.currentHeight ?? 0) : 0;

const HISTORY_PAYMENT_OPTIONS: { value: HistoryPaymentFilter; label: string }[] = [
  { value: '', label: '全部' },
  { value: 'unpaid', label: '未支付' },
  { value: 'paid', label: '已支付' },
];

const HISTORY_RESULT_OPTIONS: { value: HistoryResultFilter; label: string }[] = [
  { value: '', label: '全部' },
  { value: 'delivered', label: '已完成' },
  { value: 'cancelled', label: '已取消' },
];

function formatVoltage(value?: number | null) {
  if (value == null || Number.isNaN(Number(value))) return '-';
  return `${Number(value).toFixed(2)}V`;
}

function dialablePhone(phone?: string | null) {
  const display = (phone || '').trim();
  const dial = display.replace(/[^\d+]/g, '');
  return { display: display || '-', dial };
}

function callShelf(phone?: string | null) {
  const { dial } = dialablePhone(phone);
  if (!dial) return;
  Linking.openURL(`tel:${dial}`).catch(() => {
    Alert.alert('无法拨打', '当前设备不能打开拨号盘');
  });
}

function openRidingNavigation(address?: string | null) {
  const destination = (address || '').trim();
  if (!destination) {
    Alert.alert('无法导航', '该订单没有地址');
    return;
  }
  const url = `baidumap://map/direction?destination=${encodeURIComponent(`name:${destination}`)}&mode=riding&src=andr.banfu.waterdelivery`;
  Linking.openURL(url).catch(() => {
    Alert.alert('无法导航', '请先安装百度地图');
  });
}

function LabeledValue({
  label,
  value,
  valueStyle,
  onPress,
}: {
  label: string;
  value: string;
  valueStyle?: TextStyle;
  onPress?: () => void;
}) {
  return (
    <Text style={styles.fieldLabel}>
      {label}{' '}
      <Text style={[styles.fieldValue, valueStyle]} onPress={onPress}>{value}</Text>
    </Text>
  );
}

function formatDateTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function formatSchedule(value?: string | null) {
  if (!value) return '无';
  const text = formatDateTime(value);
  return text.length >= 16 ? text.slice(0, 16) : text;
}

function scheduleParts(value?: string | null) {
  if (!value) return { date: '', time: '' };
  const text = formatDateTime(value);
  return { date: text.slice(0, 10), time: text.slice(11, 16) };
}

function shiftDate(days: number) {
  const date = new Date();
  date.setDate(date.getDate() + days);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

function formatDatePart(date: Date) {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

function formatTimePart(date: Date) {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function schedulePickerDate(dateText: string, timeText: string) {
  const date = /^\d{4}-\d{2}-\d{2}$/.test(dateText) ? dateText : shiftDate(0);
  const time = /^\d{2}:\d{2}$/.test(timeText) ? timeText : '09:00';
  const parsed = new Date(`${date}T${time}:00`);
  return Number.isNaN(parsed.getTime()) ? new Date() : parsed;
}

function LoginScreen({ onLogin }: { onLogin: (user: User) => void }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [autoLogin, setAutoLogin] = useState(false);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    getSavedLogin().then(saved => {
      if (cancelled || !saved) return;
      setUsername(saved.username);
      setPassword(saved.password);
      setAutoLogin(true);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  const submit = async () => {
    if (!username.trim() || !password) {
      Alert.alert('提示', '请输入用户名和密码');
      return;
    }
    setLoading(true);
    try {
      onLogin(await login(username.trim(), password, autoLogin));
    } catch (error) {
      Alert.alert('登录失败', error instanceof Error ? error.message : '请稍后重试');
    } finally {
      setLoading(false);
    }
  };

  return (
    <SafeAreaView style={styles.loginPage}>
      <View style={styles.loginCard}>
        <Text style={styles.brand}>搬夫送水</Text>
        <Text style={styles.mutedCenter}>登录后处理待配送订单</Text>
        <TextInput
          style={styles.input}
          placeholder="用户名"
          value={username}
          onChangeText={setUsername}
          autoCapitalize="none"
        />
        <TextInput
          style={styles.input}
          placeholder="密码"
          value={password}
          onChangeText={setPassword}
          secureTextEntry
        />
        <Pressable style={styles.rememberRow} onPress={() => setAutoLogin(value => !value)}>
          <View style={[styles.checkbox, autoLogin && styles.checkboxOn]}>
            {autoLogin ? <Text style={styles.checkboxMark}>✓</Text> : null}
          </View>
          <Text style={styles.rememberText}>自动登录</Text>
        </Pressable>
        <Pressable style={styles.solidButton} onPress={submit} disabled={loading}>
          <Text style={styles.solidButtonText}>{loading ? '登录中...' : '登录'}</Text>
        </Pressable>
      </View>
    </SafeAreaView>
  );
}

function paymentLabel(status?: WaterOrderPaymentStatus | null) {
  return status === 'paid' ? '已支付' : '未支付';
}

function FilterChipRow<T extends string>({
  options,
  value,
  onChange,
  compact = false,
}: {
  options: { value: T; label: string }[];
  value: T;
  onChange: (next: T) => void;
  compact?: boolean;
}) {
  return (
    <View style={[styles.filterChipRow, compact && styles.filterChipRowCompact]}>
      {options.map(option => {
        const active = value === option.value;
        return (
          <Pressable
            key={option.label + String(option.value)}
            style={[
              styles.filterChip,
              compact && styles.filterChipCompact,
              active && styles.filterChipActive,
            ]}
            onPress={() => onChange(option.value)}
          >
            <Text
              style={[
                styles.filterChipText,
                compact && styles.filterChipTextCompact,
                active && styles.filterChipTextActive,
              ]}
            >
              {option.label}
            </Text>
          </Pressable>
        );
      })}
    </View>
  );
}

function OrderCard({
  order,
  alt = false,
  onStartDelivery,
  onDeliver,
  onCancel,
  onTogglePayment,
  onEditSettings,
  showNavigation = true,
}: {
  order: WaterOrder;
  alt?: boolean;
  onStartDelivery?: (order: WaterOrder) => void;
  onDeliver?: (order: WaterOrder) => void;
  onCancel?: (order: WaterOrder) => void;
  onTogglePayment?: (order: WaterOrder) => void;
  onEditSettings?: (order: WaterOrder) => void;
  showNavigation?: boolean;
}) {
  const phone = dialablePhone(order.shelf_phone);
  const address = order.address || `${order.station_name || ''} ${order.shelf_iccid}`.trim();
  const lowVoltage = Number(order.voltage) < LOW_VOLTAGE_THRESHOLD;
  const remark = (order.remark || '').trim();
  return (
    <View style={[styles.orderRow, alt && styles.orderRowAlt]}>
      <Text style={styles.orderTime}>{formatDateTime(order.created_at)}</Text>
      <View style={styles.addressRow}>
        <Text style={styles.orderAddress}>{address || '暂无地址'}</Text>
        {showNavigation ? (
          <Pressable style={styles.navButton} onPress={() => openRidingNavigation(address)}>
            <Text style={styles.navButtonText}>导航</Text>
          </Pressable>
        ) : null}
      </View>
      <LabeledValue
        label="商品"
        value={`${order.product_name || '未命名产品'}*${order.requested_quantity};`}
      />
      <LabeledValue label="微信" value={order.shelf_wechat || '-'} />
      <LabeledValue
        label="电话"
        value={phone.display}
        valueStyle={phone.dial ? styles.fieldValuePhone : undefined}
        onPress={phone.dial ? () => callShelf(order.shelf_phone) : undefined}
      />
      <LabeledValue
        label="预约配送"
        value={formatSchedule(order.scheduled_at)}
        valueStyle={onEditSettings ? styles.fieldValueAccent : undefined}
        onPress={onEditSettings ? () => onEditSettings(order) : undefined}
      />
      <LabeledValue
        label="订单备注"
        value={remark || '无'}
        valueStyle={onEditSettings ? styles.fieldValueAccent : undefined}
        onPress={onEditSettings ? () => onEditSettings(order) : undefined}
      />
      <View style={styles.orderFooter}>
        <View style={styles.orderStatus}>
          {order.status === 'pending' || order.status === 'delivering' ? (
            <LabeledValue label="触发余量" value={String(order.trigger_quantity)} valueStyle={styles.fieldValueAccent} />
          ) : (
            <Text style={styles.fieldLabel}>
              {order.delivered_quantity != null
                ? '实送 '
                : order.status === 'delivered'
                  ? '已送达'
                  : '已取消'}
              {order.delivered_quantity != null ? (
                <Text style={styles.fieldValueAccent}>{order.delivered_quantity}</Text>
              ) : null}
            </Text>
          )}
          <LabeledValue
            label="电压"
            value={formatVoltage(order.voltage)}
            valueStyle={lowVoltage ? styles.fieldValueWarning : undefined}
          />
          {onTogglePayment ? (
            <Pressable onPress={() => onTogglePayment(order)} hitSlop={6}>
              <Text style={styles.fieldLabel}>
                支付{' '}
                <Text style={order.payment_status === 'paid' ? styles.fieldValuePaid : styles.fieldValueUnpaid}>
                  {paymentLabel(order.payment_status)}
                </Text>
              </Text>
            </Pressable>
          ) : (
            <LabeledValue
              label="支付"
              value={paymentLabel(order.payment_status)}
              valueStyle={order.payment_status === 'paid' ? styles.fieldValuePaid : styles.fieldValueUnpaid}
            />
          )}
        </View>
        <View style={styles.orderActions}>
          {onStartDelivery ? (
            <Pressable style={styles.outlineButton} onPress={() => onStartDelivery(order)}>
              <Text style={styles.outlineButtonText}>去配送</Text>
            </Pressable>
          ) : null}
          {onDeliver ? (
            <Pressable style={styles.outlineButton} onPress={() => onDeliver(order)}>
              <Text style={styles.outlineButtonText}>确认送达</Text>
            </Pressable>
          ) : null}
          {onCancel ? (
            <Pressable style={styles.cancelOutlineButton} onPress={() => onCancel(order)}>
              <Text style={styles.cancelOutlineButtonText}>取消</Text>
            </Pressable>
          ) : null}
        </View>
      </View>
    </View>
  );
}

export default function App() {
  const [user, setUser] = useState<User | null>(null);
  const [restoring, setRestoring] = useState(true);
  const [tab, setTab] = useState<Tab>('pending');
  const [orders, setOrders] = useState<WaterOrder[]>([]);
  const [loading, setLoading] = useState(false);
  const [deliveryOrder, setDeliveryOrder] = useState<WaterOrder | null>(null);
  const [deliveryQuantity, setDeliveryQuantity] = useState('');
  const [deliveryPaymentStatus, setDeliveryPaymentStatus] = useState<WaterOrderPaymentStatus>('unpaid');
  const [startDeliveryOrder, setStartDeliveryOrder] = useState<WaterOrder | null>(null);
  const [startDeliveryPaymentStatus, setStartDeliveryPaymentStatus] = useState<WaterOrderPaymentStatus>('unpaid');
  const [settingsOrder, setSettingsOrder] = useState<WaterOrder | null>(null);
  const [settingsDate, setSettingsDate] = useState('');
  const [settingsTime, setSettingsTime] = useState('');
  const [settingsRemark, setSettingsRemark] = useState('');
  const [schedulePicker, setSchedulePicker] = useState<'date' | 'time' | null>(null);
  const [keyboardOffset, setKeyboardOffset] = useState(0);
  const [orderPage, setOrderPage] = useState(1);
  const [orderHasMore, setOrderHasMore] = useState(true);
  const [orderLoadingMore, setOrderLoadingMore] = useState(false);
  const [historyPaymentFilter, setHistoryPaymentFilter] = useState<HistoryPaymentFilter>('');
  const [historyResultFilter, setHistoryResultFilter] = useState<HistoryResultFilter>('');
  const [historySearchDraft, setHistorySearchDraft] = useState('');
  const [historySearchQuery, setHistorySearchQuery] = useState('');
  const shelfSearchRequestId = useRef(0);
  const orderLoadingMoreLock = useRef(false);
  const manualLogoutRef = useRef(false);
  const reauthLock = useRef(false);
  const userRef = useRef<User | null>(null);
  const [autoLoginOn, setAutoLoginOn] = useState(false);
  const deliveryQuantityInputRef = useRef<TextInput>(null);
  const appStateRef = useRef(AppState.currentState);

  const handleAuthExpired = useCallback(() => {
    setUser(null);
    setTab('pending');
    setOrders([]);
    setDeliveryOrder(null);
    setStartDeliveryOrder(null);
    setSettingsOrder(null);
    setSchedulePicker(null);
  }, []);

  userRef.current = user;

  const handleRequestError = useCallback((error: unknown, fallbackTitle = '加载失败') => {
    if (error instanceof AuthExpiredError) {
      if (manualLogoutRef.current || reauthLock.current) {
        handleAuthExpired();
        return;
      }
      reauthLock.current = true;
      tryAutoLogin()
        .then(nextUser => {
          if (nextUser) {
            setUser(nextUser);
            setAutoLoginOn(true);
            return;
          }
          handleAuthExpired();
        })
        .finally(() => {
          reauthLock.current = false;
        });
      return;
    }
    Alert.alert(fallbackTitle, error instanceof Error ? error.message : '请稍后重试');
  }, [handleAuthExpired]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const restored = await restoreUser();
      const nextUser = restored ?? await tryAutoLogin();
      if (cancelled) return;
      const saved = nextUser ? await getSavedLogin() : null;
      if (cancelled) return;
      setUser(nextUser);
      setAutoLoginOn(!!saved);
      if (nextUser) setTab('pending');
    })()
      .catch(() => {
        if (!cancelled) setUser(null);
      })
      .finally(() => {
        if (!cancelled) setRestoring(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const subscription = AppState.addEventListener('change', nextState => {
      const prev = appStateRef.current;
      appStateRef.current = nextState;
      if (!prev.match(/inactive|background/) || nextState !== 'active') return;
      if (manualLogoutRef.current || !userRef.current) return;
      restoreUser().then(async nextUser => {
        const resolved = nextUser ?? await tryAutoLogin();
        if (!resolved) {
          handleAuthExpired();
          return;
        }
        setUser(resolved);
      });
    });
    return () => subscription.remove();
  }, [handleAuthExpired]);

  useEffect(() => {
    const showEvent = Platform.OS === 'ios' ? 'keyboardWillShow' : 'keyboardDidShow';
    const hideEvent = Platform.OS === 'ios' ? 'keyboardWillHide' : 'keyboardDidHide';
    const showSub = Keyboard.addListener(showEvent, event => {
      setKeyboardOffset(event.endCoordinates.height);
    });
    const hideSub = Keyboard.addListener(hideEvent, () => setKeyboardOffset(0));
    return () => {
      showSub.remove();
      hideSub.remove();
    };
  }, []);

  useEffect(() => {
    if (!deliveryOrder) return;
    const timer = setTimeout(() => {
      deliveryQuantityInputRef.current?.focus();
    }, 250);
    return () => clearTimeout(timer);
  }, [deliveryOrder]);

  useEffect(() => {
    if (!user) return;
    let removeListeners: (() => void) | undefined;
    setupPushNotifications(() => setTab('pending'))
      .then(cleanup => {
        removeListeners = cleanup;
      })
      .catch(error => console.warn('个推注册失败', error));
    return () => removeListeners?.();
  }, [user]);

  const loadOrders = useCallback(async (page = 1, append = false) => {
    if (!user || (tab !== 'pending' && tab !== 'delivering' && tab !== 'history')) return;
    const requestId = ++shelfSearchRequestId.current;
    if (append) {
      if (orderLoadingMoreLock.current || !orderHasMore) return;
      orderLoadingMoreLock.current = true;
      setOrderLoadingMore(true);
    } else {
      setLoading(true);
      setOrderHasMore(true);
    }
    try {
      const filters = tab === 'history'
        ? {
            paymentStatus: historyPaymentFilter,
            result: historyResultFilter,
            search: historySearchQuery,
          }
        : {};
      const result = await getOrders(tab, page, PAGE_SIZE, filters);
      if (requestId !== shelfSearchRequestId.current) return;
      setOrders(prev => {
        if (!append) return result.orders;
        const seen = new Set(prev.map(item => item.id));
        return [...prev, ...result.orders.filter(item => !seen.has(item.id))];
      });
      setOrderPage(page);
      const loaded = page * PAGE_SIZE;
      setOrderHasMore(result.orders.length >= PAGE_SIZE && loaded < result.total_count);
    } catch (error) {
      if (requestId !== shelfSearchRequestId.current) return;
      handleRequestError(error);
    } finally {
      if (requestId === shelfSearchRequestId.current) {
        setLoading(false);
        setOrderLoadingMore(false);
        orderLoadingMoreLock.current = false;
      }
    }
  }, [
    tab,
    user,
    orderHasMore,
    historyPaymentFilter,
    historyResultFilter,
    historySearchQuery,
    handleRequestError,
  ]);

  useEffect(() => {
    if (!user || tab === 'more' || tab === 'shelves') return;
    if (tab === 'pending' || tab === 'delivering' || tab === 'history') {
      loadOrders(1, false);
    }
  }, [
    user,
    tab,
    historyPaymentFilter,
    historyResultFilter,
    historySearchQuery,
  ]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadMoreOrders = () => {
    if ((tab !== 'pending' && tab !== 'delivering' && tab !== 'history') || loading || orderLoadingMore || !orderHasMore) return;
    loadOrders(orderPage + 1, true);
  };

  const switchTab = (next: Tab) => {
    setTab(next);
  };

  const applyHistorySearchNow = (value?: string) => {
    const next = (value ?? historySearchDraft).trim();
    setHistorySearchDraft(next);
    setHistorySearchQuery(next);
    Keyboard.dismiss();
  };

  const clearHistorySearch = () => {
    setHistorySearchDraft('');
    setHistorySearchQuery('');
    Keyboard.dismiss();
  };

  const finishDelivery = async () => {
    if (!deliveryOrder) return;
    const quantity = Number(deliveryQuantity);
    if (!Number.isInteger(quantity) || quantity <= 0) {
      Alert.alert('提示', '送水数量必须是大于0的整数');
      return;
    }
    try {
      await deliverOrder(deliveryOrder.id, quantity, deliveryPaymentStatus);
      setDeliveryOrder(null);
      setDeliveryQuantity('');
      setDeliveryPaymentStatus('unpaid');
      Alert.alert('完成', '订单已送达，货架余量已增加');
      loadOrders(1, false);
    } catch (error) {
      handleRequestError(error, '操作失败');
    }
  };

  const handleStartDelivery = (order: WaterOrder) => {
    setStartDeliveryOrder(order);
    setStartDeliveryPaymentStatus(order.payment_status === 'paid' ? 'paid' : 'unpaid');
  };

  const confirmStartDelivery = async () => {
    if (!startDeliveryOrder) return;
    try {
      await startDelivery(startDeliveryOrder.id, startDeliveryPaymentStatus);
      setStartDeliveryOrder(null);
      setTab('delivering');
      Alert.alert('成功', '订单已进入配送中');
    } catch (error) {
      handleRequestError(error, '操作失败');
    }
  };

  const openOrderSettings = (order: WaterOrder) => {
    const parts = scheduleParts(order.scheduled_at);
    setSchedulePicker(null);
    setSettingsOrder(order);
    setSettingsDate(parts.date);
    setSettingsTime(parts.time);
    setSettingsRemark(order.remark || '');
  };

  const onSchedulePicked = (event: DateTimePickerEvent, selected?: Date) => {
    const mode = schedulePicker;
    if (Platform.OS === 'android') setSchedulePicker(null);
    if (event.type === 'dismissed' || !selected || !mode) return;
    if (mode === 'date') {
      setSettingsDate(formatDatePart(selected));
      setSettingsTime(current => current || '09:00');
      return;
    }
    setSettingsTime(formatTimePart(selected));
    setSettingsDate(current => current || shiftDate(0));
  };

  const confirmOrderSettings = async () => {
    if (!settingsOrder) return;
    const date = settingsDate.trim();
    const time = settingsTime.trim();
    let scheduledAt: string | null = null;
    if (date || time) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !/^\d{2}:\d{2}$/.test(time)) {
        Alert.alert('提示', '预约时间请填写日期和时间，或全部留空表示无');
        return;
      }
      const [hour, minute] = time.split(':').map(Number);
      if (hour > 23 || minute > 59 || Number.isNaN(new Date(`${date}T${time}:00`).getTime())) {
        Alert.alert('提示', '预约时间无效');
        return;
      }
      scheduledAt = `${date}T${time}:00`;
    }
    const remark = settingsRemark.trim();
    if (remark.length > 255) {
      Alert.alert('提示', '订单备注不能超过 255 字');
      return;
    }
    try {
      await updateOrderSettings(settingsOrder.id, scheduledAt, remark);
      setSettingsOrder(null);
      Keyboard.dismiss();
      loadOrders(1, false);
    } catch (error) {
      handleRequestError(error, '保存失败');
    }
  };

  const handleCancelOrder = (order: WaterOrder) => {
    Alert.alert('取消订单', `确认取消订单 ${order.order_no}？`, [
      { text: '返回', style: 'cancel' },
      {
        text: '确认取消',
        style: 'destructive',
        onPress: async () => {
          try {
            await cancelOrder(order.id);
            Alert.alert('已取消', '订单已取消');
            loadOrders(1, false);
          } catch (error) {
            handleRequestError(error, '取消失败');
          }
        },
      },
    ]);
  };

  const handleTogglePayment = (order: WaterOrder) => {
    const next: WaterOrderPaymentStatus = order.payment_status === 'paid' ? 'unpaid' : 'paid';
    Alert.alert('修改支付状态', `确认将订单 ${order.order_no} 改为「${paymentLabel(next)}」？`, [
      { text: '取消', style: 'cancel' },
      {
        text: '确认',
        onPress: async () => {
          try {
            await updateOrderPayment(order.id, next);
            loadOrders(1, false);
          } catch (error) {
            handleRequestError(error, '更新失败');
          }
        },
      },
    ]);
  };

  const handleShelfOrder = async (shelf: Shelf) => {
    try {
      const active = await getActiveOrder(shelf.id);
      if (active) {
        setTab(active.status === 'delivering' ? 'delivering' : 'pending');
        Alert.alert('已有订单', `订单 ${active.order_no} 已在${active.status === 'delivering' ? '配送中' : '待配送'}列表中`);
        return;
      }
      Alert.alert(
        '生成送水订单',
        `为货架 ${shelf.iccid} 生成订单？建议配送 ${Math.max((shelf.total_quantity || 0) - (shelf.current_quantity || 0), 1)} 件。`,
        [
          { text: '取消', style: 'cancel' },
          {
            text: '生成',
            onPress: async () => {
              try {
                await createOrder(shelf.id);
                setTab('pending');
                Alert.alert('成功', '送水订单已生成');
              } catch (error) {
                handleRequestError(error, '生成失败');
              }
            },
          },
        ],
      );
    } catch (error) {
      handleRequestError(error, '操作失败');
    }
  };

  const confirmLogout = () => {
    Alert.alert('注销', '确认退出当前账号？', [
      { text: '取消', style: 'cancel' },
      {
        text: '注销',
        style: 'destructive',
        onPress: async () => {
          manualLogoutRef.current = true;
          await logout();
          setUser(null);
        },
      },
    ]);
  };

  if (restoring) {
    return <View style={styles.center}><ActivityIndicator size="large" color={ACCENT} /></View>;
  }
  if (!user) {
    return (
      <LoginScreen
        onLogin={nextUser => {
          manualLogoutRef.current = false;
          setUser(nextUser);
          setTab('pending');
          getSavedLogin().then(saved => setAutoLoginOn(!!saved));
        }}
      />
    );
  }

  return (
    <SafeAreaView style={[styles.page, { paddingTop: TOP_INSET }]}>
      <StatusBar style="dark" />

      <View style={styles.tabs}>
        {([
          ['pending', '待配送'],
          ['delivering', '配送中'],
          ['history', '历史'],
          ['shelves', '货架'],
          ['more', '更多'],
        ] as [Tab, string][]).map(([value, label]) => {
          const active = tab === value;
          return (
            <Pressable key={value} style={styles.tab} onPress={() => switchTab(value)}>
              <Text style={[styles.tabText, active && styles.activeTabText]}>{label}</Text>
              <View style={[styles.tabUnderline, active && styles.activeTabUnderline]} />
            </Pressable>
          );
        })}
      </View>

      {tab === 'more' ? (
        <View style={styles.morePage}>
          <View style={styles.moreBody}>
            <Text style={styles.moreBrand}>搬夫送水</Text>
            <Text style={styles.moreLine}>当前登录：{user.username}</Text>
            <Pressable
              style={styles.autoLoginRow}
              onPress={() => {
                if (!autoLoginOn) {
                  Alert.alert('自动登录', '退出后在登录页勾选「自动登录」，下次打开将直接进入');
                  return;
                }
                Alert.alert('关闭自动登录', '关闭后下次打开需要重新输入密码', [
                  { text: '取消', style: 'cancel' },
                  {
                    text: '关闭',
                    style: 'destructive',
                    onPress: async () => {
                      await clearAutoLogin();
                      setAutoLoginOn(false);
                    },
                  },
                ]);
              }}
            >
              <Text style={styles.moreLine}>自动登录</Text>
              <Text style={autoLoginOn ? styles.autoLoginOn : styles.moreMeta}>
                {autoLoginOn ? '已开启' : '未开启'}
              </Text>
            </Pressable>
            <Text style={styles.moreMeta}>版本号：v{APP_VERSION}</Text>
            <Text style={styles.moreMeta}>版权所有 ©搬夫科技</Text>
          </View>
          <Pressable style={styles.logoutButton} onPress={confirmLogout}>
            <Text style={styles.solidButtonText}>注销</Text>
          </Pressable>
        </View>
      ) : tab === 'shelves' ? (
        <ShelfPane
          username={user.username}
          onGenerateOrder={handleShelfOrder}
          onError={handleRequestError}
        />
      ) : (
        <View style={styles.ordersPane}>
          {tab === 'history' ? (
            <View style={styles.searchPanel}>
              <View style={styles.filterBar}>
                <FilterChipRow
                  options={HISTORY_PAYMENT_OPTIONS}
                  value={historyPaymentFilter}
                  onChange={setHistoryPaymentFilter}
                  compact
                />
                <View style={styles.filterDivider} />
                <FilterChipRow
                  options={HISTORY_RESULT_OPTIONS}
                  value={historyResultFilter}
                  onChange={setHistoryResultFilter}
                  compact
                />
              </View>
              <View style={styles.searchRow}>
                <TextInput
                  style={styles.searchInput}
                  placeholder="地址、设备号、电话、微信"
                  placeholderTextColor="#999"
                  value={historySearchDraft}
                  onChangeText={setHistorySearchDraft}
                  returnKeyType="search"
                  autoCorrect={false}
                  autoCapitalize="none"
                  clearButtonMode="never"
                  onSubmitEditing={() => applyHistorySearchNow()}
                />
                {historySearchDraft.length > 0 ? (
                  <Pressable style={styles.searchClear} onPress={clearHistorySearch} hitSlop={8}>
                    <Text style={styles.searchClearText}>清除</Text>
                  </Pressable>
                ) : null}
                <Pressable style={styles.searchButton} onPress={() => applyHistorySearchNow()}>
                  <Text style={styles.searchButtonText}>搜索</Text>
                </Pressable>
              </View>
              <Text style={styles.searchHint}>
                {(() => {
                  const parts = [
                    historyPaymentFilter === 'unpaid'
                      ? '未支付'
                      : historyPaymentFilter === 'paid'
                        ? '已支付'
                        : null,
                    historyResultFilter === 'delivered'
                      ? '已完成'
                      : historyResultFilter === 'cancelled'
                        ? '已取消'
                        : null,
                    historySearchQuery ? `「${historySearchQuery}」` : null,
                  ].filter(Boolean);
                  const prefix = parts.length > 0 ? `${parts.join(' · ')} ` : '';
                  if (loading && orders.length === 0) {
                    return parts.length > 0 ? `正在筛选${prefix}…` : '加载中…';
                  }
                  if (orderHasMore) {
                    return `${prefix}已加载 ${orders.length} 条，下滑继续加载`;
                  }
                  return `${prefix}共 ${orders.length} 条`;
                })()}
              </Text>
            </View>
          ) : null}
          <FlatList
            data={orders}
            keyExtractor={item => String(item.id)}
            keyboardShouldPersistTaps="handled"
            refreshControl={
              <RefreshControl
                refreshing={loading && orders.length === 0}
                onRefresh={() => loadOrders(1, false)}
                colors={[ACCENT]}
                tintColor={ACCENT}
              />
            }
            contentContainerStyle={styles.list}
            onEndReached={loadMoreOrders}
            onEndReachedThreshold={0.3}
            ListEmptyComponent={
              !loading ? (
                <Text style={styles.empty}>
                  {tab === 'history' && (historyPaymentFilter || historyResultFilter || historySearchQuery)
                    ? '未找到匹配的历史订单，试试换个条件'
                    : '暂无订单'}
                </Text>
              ) : (
                <View style={styles.listLoading}>
                  <ActivityIndicator color={ACCENT} />
                </View>
              )
            }
            ListFooterComponent={
              orderLoadingMore ? (
                <View style={styles.listLoading}>
                  <ActivityIndicator color={ACCENT} />
                  <Text style={styles.muted}>加载更多…</Text>
                </View>
              ) : orders.length > 0 && !orderHasMore ? (
                <Text style={styles.listEnd}>已经到底了</Text>
              ) : null
            }
            renderItem={({ item, index }) => (
              <OrderCard
                order={item}
                alt={index % 2 === 1}
                showNavigation={tab !== 'history'}
                onStartDelivery={tab === 'pending' ? handleStartDelivery : undefined}
                onDeliver={tab === 'delivering' ? order => {
                  setDeliveryOrder(order);
                  setDeliveryQuantity(String(order.requested_quantity));
                  setDeliveryPaymentStatus(order.payment_status === 'paid' ? 'paid' : 'unpaid');
                } : undefined}
                onCancel={tab === 'pending' || tab === 'delivering' ? handleCancelOrder : undefined}
                onTogglePayment={tab === 'history' ? handleTogglePayment : undefined}
                onEditSettings={tab === 'pending' || tab === 'delivering' ? openOrderSettings : undefined}
              />
            )}
          />
        </View>
      )}

      <Modal visible={!!deliveryOrder} transparent animationType="slide" onRequestClose={() => setDeliveryOrder(null)}>
        <View style={styles.modalAvoider}>
          <Pressable
            style={styles.modalBackdrop}
            onPress={() => {
              Keyboard.dismiss();
              setDeliveryOrder(null);
            }}
          >
            <Pressable
              style={[styles.modal, { marginBottom: keyboardOffset }]}
              onPress={event => event.stopPropagation()}
            >
              <Text style={styles.modalTitle}>确认送达</Text>
              <Text style={styles.orderProduct}>
                {deliveryOrder?.station_name} · {deliveryOrder?.product_name || '-'} · {deliveryOrder?.shelf_iccid}
              </Text>
              <LabeledValue label="微信" value={deliveryOrder?.shelf_wechat || '-'} />
              <LabeledValue
                label="电话"
                value={dialablePhone(deliveryOrder?.shelf_phone).display}
                valueStyle={dialablePhone(deliveryOrder?.shelf_phone).dial ? styles.fieldValuePhone : undefined}
                onPress={dialablePhone(deliveryOrder?.shelf_phone).dial ? () => callShelf(deliveryOrder?.shelf_phone) : undefined}
              />
              <LabeledValue
                label="电压"
                value={formatVoltage(deliveryOrder?.voltage)}
                valueStyle={Number(deliveryOrder?.voltage) < LOW_VOLTAGE_THRESHOLD ? styles.fieldValueWarning : undefined}
              />
              <Text style={styles.label}>本次送水数量</Text>
              <Text style={styles.quantityPreview}>
                {deliveryQuantity.trim() ? deliveryQuantity : '—'}
              </Text>
              <TextInput
                ref={deliveryQuantityInputRef}
                style={styles.quantityInput}
                value={deliveryQuantity}
                onChangeText={setDeliveryQuantity}
                keyboardType="number-pad"
                selectTextOnFocus
                autoFocus
                textAlign="center"
                onFocus={() => {
                  const length = deliveryQuantity.length;
                  requestAnimationFrame(() => {
                    deliveryQuantityInputRef.current?.setNativeProps({
                      selection: { start: 0, end: length },
                    });
                  });
                }}
              />
              <Text style={styles.label}>支付状态</Text>
              <View style={styles.paymentChoices}>
                <Pressable
                  style={[styles.paymentChoice, deliveryPaymentStatus === 'unpaid' && styles.paymentChoiceActive]}
                  onPress={() => setDeliveryPaymentStatus('unpaid')}
                >
                  <Text style={[styles.paymentChoiceText, deliveryPaymentStatus === 'unpaid' && styles.paymentChoiceTextActive]}>未支付</Text>
                </Pressable>
                <Pressable
                  style={[styles.paymentChoice, deliveryPaymentStatus === 'paid' && styles.paymentChoiceActive]}
                  onPress={() => setDeliveryPaymentStatus('paid')}
                >
                  <Text style={[styles.paymentChoiceText, deliveryPaymentStatus === 'paid' && styles.paymentChoiceTextActive]}>已支付</Text>
                </Pressable>
              </View>
              <Text style={styles.muted}>点击输入框会选中当前数量，直接输入即可覆盖。</Text>
              <View style={styles.modalActions}>
                <Pressable
                  style={styles.cancelButton}
                  onPress={() => {
                    Keyboard.dismiss();
                    setDeliveryOrder(null);
                  }}
                >
                  <Text style={styles.cancelButtonText}>取消</Text>
                </Pressable>
                <Pressable style={[styles.solidButton, styles.flexButton]} onPress={finishDelivery}>
                  <Text style={styles.solidButtonText}>确认送达</Text>
                </Pressable>
              </View>
            </Pressable>
          </Pressable>
        </View>
      </Modal>

      <Modal
        visible={!!startDeliveryOrder}
        transparent
        animationType="slide"
        onRequestClose={() => setStartDeliveryOrder(null)}
      >
        <View style={styles.modalAvoider}>
          <Pressable
            style={styles.modalBackdrop}
            onPress={() => setStartDeliveryOrder(null)}
          >
            <Pressable
              style={styles.modal}
              onPress={event => event.stopPropagation()}
            >
              <Text style={styles.modalTitle}>去配送</Text>
              <Text style={styles.orderProduct}>
                {startDeliveryOrder?.station_name} · {startDeliveryOrder?.product_name || '-'} · {startDeliveryOrder?.shelf_iccid}
              </Text>
              <Text style={styles.muted}>确认开始配送订单 {startDeliveryOrder?.order_no}？</Text>
              <Text style={styles.label}>支付状态</Text>
              <View style={styles.paymentChoices}>
                <Pressable
                  style={[styles.paymentChoice, startDeliveryPaymentStatus === 'unpaid' && styles.paymentChoiceActive]}
                  onPress={() => setStartDeliveryPaymentStatus('unpaid')}
                >
                  <Text style={[styles.paymentChoiceText, startDeliveryPaymentStatus === 'unpaid' && styles.paymentChoiceTextActive]}>未支付</Text>
                </Pressable>
                <Pressable
                  style={[styles.paymentChoice, startDeliveryPaymentStatus === 'paid' && styles.paymentChoiceActive]}
                  onPress={() => setStartDeliveryPaymentStatus('paid')}
                >
                  <Text style={[styles.paymentChoiceText, startDeliveryPaymentStatus === 'paid' && styles.paymentChoiceTextActive]}>已支付</Text>
                </Pressable>
              </View>
              <View style={styles.modalActions}>
                <Pressable style={styles.cancelButton} onPress={() => setStartDeliveryOrder(null)}>
                  <Text style={styles.cancelButtonText}>取消</Text>
                </Pressable>
                <Pressable style={[styles.solidButton, styles.flexButton]} onPress={confirmStartDelivery}>
                  <Text style={styles.solidButtonText}>确认去配送</Text>
                </Pressable>
              </View>
            </Pressable>
          </Pressable>
        </View>
      </Modal>

      <Modal
        visible={!!settingsOrder}
        transparent
        animationType="slide"
        onRequestClose={() => {
          setSchedulePicker(null);
          setSettingsOrder(null);
        }}
      >
        <View style={styles.modalAvoider}>
          <Pressable
            style={styles.modalBackdrop}
            onPress={() => {
              Keyboard.dismiss();
              setSchedulePicker(null);
              setSettingsOrder(null);
            }}
          >
            <Pressable
              style={[styles.modal, { marginBottom: keyboardOffset }]}
              onPress={event => event.stopPropagation()}
            >
              <Text style={styles.modalTitle}>预约与备注</Text>
              <Text style={styles.orderProduct}>
                {settingsOrder?.station_name} · {settingsOrder?.shelf_iccid}
              </Text>
              <Text style={styles.label}>预约配送时间</Text>
              <View style={styles.scheduleChips}>
                {[
                  ['今天', 0],
                  ['明天', 1],
                  ['后天', 2],
                ].map(([label, offset]) => (
                  <Pressable
                    key={String(label)}
                    style={[styles.filterChip, styles.filterChipCompact]}
                    onPress={() => {
                      setSettingsDate(shiftDate(Number(offset)));
                      if (!settingsTime) setSettingsTime('09:00');
                    }}
                  >
                    <Text style={[styles.filterChipText, styles.filterChipTextCompact]}>{label}</Text>
                  </Pressable>
                ))}
                <Pressable
                  style={[styles.filterChip, styles.filterChipCompact]}
                  onPress={() => {
                    setSettingsDate('');
                    setSettingsTime('');
                  }}
                >
                  <Text style={[styles.filterChipText, styles.filterChipTextCompact]}>清除</Text>
                </Pressable>
              </View>
              <View style={styles.scheduleInputs}>
                <Pressable
                  style={[styles.input, styles.scheduleDateInput, styles.schedulePicker]}
                  onPress={() => setSchedulePicker('date')}
                >
                  <Text style={settingsDate ? styles.schedulePickerText : styles.schedulePickerPlaceholder}>
                    {settingsDate || '选择日期'}
                  </Text>
                </Pressable>
                <Pressable
                  style={[styles.input, styles.scheduleTimeInput, styles.schedulePicker]}
                  onPress={() => setSchedulePicker('time')}
                >
                  <Text style={settingsTime ? styles.schedulePickerText : styles.schedulePickerPlaceholder}>
                    {settingsTime || '选择时间'}
                  </Text>
                </Pressable>
              </View>
              <Text style={styles.label}>订单备注</Text>
              <TextInput
                style={[styles.input, styles.remarkInput]}
                value={settingsRemark}
                onChangeText={setSettingsRemark}
                placeholder="无"
                placeholderTextColor="#999"
                multiline
                maxLength={255}
              />
              <View style={styles.modalActions}>
                <Pressable
                  style={styles.cancelButton}
                  onPress={() => {
                    setSchedulePicker(null);
                    setSettingsOrder(null);
                  }}
                >
                  <Text style={styles.cancelButtonText}>取消</Text>
                </Pressable>
                <Pressable style={[styles.solidButton, styles.flexButton]} onPress={confirmOrderSettings}>
                  <Text style={styles.solidButtonText}>保存</Text>
                </Pressable>
              </View>
            </Pressable>
          </Pressable>
        </View>
      </Modal>
      {schedulePicker && settingsOrder ? (
        <DateTimePicker
          value={schedulePickerDate(settingsDate, settingsTime)}
          mode={schedulePicker}
          display="default"
          is24Hour
          positiveButton={{ label: '确定', textColor: ACCENT }}
          negativeButton={{ label: '取消', textColor: '#666' }}
          onChange={onSchedulePicked}
        />
      ) : null}
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  page: { flex: 1, backgroundColor: '#fff' },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', backgroundColor: '#fff' },
  loginPage: { flex: 1, justifyContent: 'center', padding: 24, backgroundColor: '#fff' },
  loginCard: { gap: 14 },
  rememberRow: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  checkbox: {
    width: 18,
    height: 18,
    borderRadius: 4,
    borderWidth: 1,
    borderColor: '#ccc',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#fff',
  },
  checkboxOn: { borderColor: ACCENT, backgroundColor: ACCENT },
  checkboxMark: { color: '#fff', fontSize: 12, fontWeight: '700', lineHeight: 14 },
  rememberText: { color: '#333', fontSize: 15 },
  brand: { fontSize: 28, fontWeight: '700', color: '#222', textAlign: 'center' },
  muted: { color: '#888', fontSize: 13 },
  mutedCenter: { color: '#888', fontSize: 13, textAlign: 'center', marginBottom: 8 },
  tabs: {
    flexDirection: 'row',
    backgroundColor: '#fff',
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#e5e5e5',
    paddingHorizontal: 4,
  },
  tab: {
    flex: 1,
    alignItems: 'center',
    paddingTop: 12,
    paddingBottom: 0,
  },
  tabText: { color: '#666', fontSize: 13, fontWeight: '500', paddingBottom: 10 },
  activeTabText: { color: ACCENT, fontWeight: '700' },
  tabUnderline: { alignSelf: 'stretch', height: 2, backgroundColor: 'transparent' },
  activeTabUnderline: { backgroundColor: ACCENT },
  shelvesPane: { flex: 1 },
  ordersPane: { flex: 1 },
  searchPanel: {
    backgroundColor: '#fff',
    paddingHorizontal: 12,
    paddingTop: 10,
    paddingBottom: 8,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#e5e5e5',
    gap: 6,
  },
  filterBar: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'flex-start',
    gap: 8,
  },
  filterDivider: {
    width: StyleSheet.hairlineWidth,
    alignSelf: 'stretch',
    backgroundColor: '#d0d5dd',
    marginHorizontal: 2,
  },
  filterChipRow: { flexDirection: 'row', gap: 8 },
  filterChipRowCompact: { gap: 3, flexShrink: 1 },
  filterChip: {
    borderWidth: 1,
    borderColor: '#ddd',
    borderRadius: 6,
    paddingHorizontal: 12,
    paddingVertical: 6,
    backgroundColor: '#fafafa',
  },
  filterChipCompact: {
    paddingHorizontal: 6,
    paddingVertical: 3,
    borderRadius: 4,
  },
  filterChipActive: {
    borderColor: ACCENT,
    backgroundColor: '#eff6ff',
  },
  filterChipText: { color: '#666', fontSize: 13, fontWeight: '500' },
  filterChipTextCompact: { fontSize: 11, lineHeight: 14 },
  filterChipTextActive: { color: ACCENT, fontWeight: '700' },
  searchRow: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  searchInput: {
    flex: 1,
    borderWidth: 1,
    borderColor: '#ddd',
    borderRadius: 6,
    paddingHorizontal: 12,
    paddingVertical: 9,
    backgroundColor: '#fafafa',
    fontSize: 14,
    color: '#222',
  },
  searchClear: { paddingHorizontal: 4, paddingVertical: 8 },
  searchClearText: { color: '#888', fontWeight: '600', fontSize: 13 },
  searchButton: {
    borderWidth: 1,
    borderColor: ACCENT,
    borderRadius: 6,
    paddingHorizontal: 14,
    paddingVertical: 9,
  },
  searchButtonText: { color: ACCENT, fontWeight: '700', fontSize: 14 },
  searchHint: { color: '#999', fontSize: 12 },
  list: { flexGrow: 1, backgroundColor: '#fff' },
  orderRow: {
    paddingHorizontal: 16,
    paddingVertical: 14,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#e8e8e8',
    backgroundColor: '#fff',
    gap: 6,
  },
  orderRowAlt: {
    backgroundColor: '#f3f4f6',
  },
  orderTime: { color: '#999', fontSize: 13 },
  addressRow: { flexDirection: 'row', alignItems: 'flex-start', gap: 10 },
  orderAddress: { flex: 1, color: '#222', fontSize: 16, fontWeight: '700', lineHeight: 22 },
  navButton: {
    backgroundColor: ACCENT,
    borderRadius: 6,
    paddingHorizontal: 12,
    paddingVertical: 6,
    marginTop: 1,
  },
  navButtonText: { color: '#fff', fontWeight: '700', fontSize: 13 },
  orderProduct: { color: '#333', fontSize: 15, lineHeight: 21 },
  fieldLabel: { color: '#666', fontSize: 14, lineHeight: 21, fontWeight: '400' },
  fieldValue: { color: '#222', fontWeight: '700' },
  fieldValueAccent: { color: ACCENT, fontWeight: '700' },
  fieldValuePhone: { color: ACCENT, fontWeight: '700' },
  fieldValueWarning: { color: DANGER, fontWeight: '700' },
  fieldValuePaid: { color: '#059669', fontWeight: '700' },
  fieldValueUnpaid: { color: '#d97706', fontWeight: '700' },
  orderContact: { color: '#666', fontSize: 13, lineHeight: 18 },
  orderPhone: { color: ACCENT, fontSize: 14, fontWeight: '700', lineHeight: 20 },
  orderStatus: { flex: 1, gap: 6 },
  orderActions: { gap: 8, alignItems: 'stretch' },
  orderFooter: {
    flexDirection: 'row',
    alignItems: 'flex-end',
    justifyContent: 'space-between',
    gap: 12,
  },
  orderMeta: { color: ACCENT, fontSize: 13, flex: 1 },
  warningText: { color: DANGER, fontWeight: '700' },
  outlineButton: {
    borderWidth: 1,
    borderColor: ACCENT,
    borderRadius: 6,
    paddingHorizontal: 16,
    paddingVertical: 7,
    minWidth: 88,
    alignItems: 'center',
    backgroundColor: '#eff6ff',
  },
  outlineButtonText: { color: ACCENT, fontWeight: '700', fontSize: 14 },
  cancelOutlineButton: {
    borderWidth: 1,
    borderColor: '#ccc',
    borderRadius: 6,
    paddingHorizontal: 16,
    paddingVertical: 7,
    minWidth: 88,
    alignItems: 'center',
  },
  cancelOutlineButtonText: { color: '#666', fontWeight: '600', fontSize: 14 },
  paymentChoices: { flexDirection: 'row', gap: 10, marginTop: 4 },
  paymentChoice: {
    flex: 1,
    borderWidth: 1,
    borderColor: '#ddd',
    borderRadius: 8,
    paddingVertical: 10,
    alignItems: 'center',
    backgroundColor: '#fff',
  },
  paymentChoiceActive: { borderColor: ACCENT, backgroundColor: '#eff6ff' },
  paymentChoiceText: { color: '#666', fontWeight: '600' },
  paymentChoiceTextActive: { color: ACCENT, fontWeight: '700' },
  solidButton: {
    backgroundColor: ACCENT,
    borderRadius: 8,
    paddingVertical: 13,
    alignItems: 'center',
  },
  solidButtonText: { color: '#fff', fontWeight: '700', fontSize: 16 },
  input: {
    borderWidth: 1,
    borderColor: '#ddd',
    borderRadius: 8,
    paddingHorizontal: 12,
    paddingVertical: 10,
    fontSize: 16,
    color: '#222',
    backgroundColor: '#fafafa',
  },
  scheduleChips: { flexDirection: 'row', flexWrap: 'wrap', gap: 6, marginTop: 8 },
  scheduleInputs: { flexDirection: 'row', gap: 8, marginTop: 8 },
  scheduleDateInput: { flex: 1.4 },
  scheduleTimeInput: { flex: 1 },
  schedulePicker: { justifyContent: 'center' },
  schedulePickerText: { color: '#222', fontSize: 16 },
  schedulePickerPlaceholder: { color: '#999', fontSize: 16 },
  remarkInput: { minHeight: 72, textAlignVertical: 'top', marginTop: 8 },
  label: { color: '#333', fontWeight: '600', marginTop: 8 },
  empty: { textAlign: 'center', marginTop: 80, color: '#999' },
  listLoading: { paddingVertical: 16, alignItems: 'center', gap: 8 },
  listEnd: { textAlign: 'center', color: '#999', paddingVertical: 16, fontSize: 12 },
  morePage: {
    flex: 1,
    backgroundColor: '#fff',
    paddingHorizontal: 28,
    paddingTop: 48,
    paddingBottom: 28,
    justifyContent: 'space-between',
  },
  moreBody: { alignItems: 'center', gap: 14 },
  moreBrand: { fontSize: 28, fontWeight: '700', color: '#222', marginBottom: 8 },
  moreLine: { fontSize: 16, color: '#333' },
  moreMeta: { fontSize: 13, color: '#999', marginTop: 4 },
  autoLoginRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    alignSelf: 'stretch',
    marginTop: 8,
    paddingVertical: 8,
  },
  autoLoginOn: { fontSize: 14, color: ACCENT, fontWeight: '600' },
  logoutButton: {
    backgroundColor: ACCENT,
    borderRadius: 8,
    paddingVertical: 14,
    alignItems: 'center',
  },
  modalAvoider: { flex: 1 },
  modalBackdrop: { flex: 1, backgroundColor: 'rgba(0,0,0,0.35)', justifyContent: 'flex-end' },
  modal: {
    backgroundColor: '#fff',
    borderTopLeftRadius: 16,
    borderTopRightRadius: 16,
    padding: 22,
    gap: 8,
  },
  modalTitle: { color: '#222', fontSize: 18, fontWeight: '700' },
  quantityPreview: {
    textAlign: 'center',
    fontSize: 40,
    lineHeight: 48,
    fontWeight: '700',
    color: '#222',
    paddingVertical: 6,
  },
  quantityInput: {
    borderWidth: 1,
    borderColor: '#ddd',
    borderRadius: 8,
    paddingHorizontal: 12,
    paddingVertical: 14,
    backgroundColor: '#fff',
    fontSize: 22,
    fontWeight: '700',
    color: '#222',
  },
  modalActions: { flexDirection: 'row', gap: 10, marginTop: 12 },
  cancelButton: {
    flex: 1,
    backgroundColor: '#f3f3f3',
    borderRadius: 8,
    paddingVertical: 13,
    alignItems: 'center',
  },
  cancelButtonText: { color: '#444', fontWeight: '600' },
  flexButton: { flex: 1, marginTop: 0 },
});
