import { StatusBar } from 'expo-status-bar';
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
  View,
} from 'react-native';
import {
  AuthExpiredError,
  createOrder,
  deliverOrder,
  getActiveOrder,
  getOrders,
  getShelves,
  login,
  logout,
  restoreUser,
} from './src/api';
import { setupPushNotifications } from './src/notifications';
import { Shelf, User, WaterOrder } from './src/types';

type Tab = 'pending' | 'shelves' | 'history' | 'more';
const PAGE_SIZE = 20;
const SHELF_PAGE_SIZE = 10;
const SHELF_SEARCH_DEBOUNCE_MS = 400;
const APP_VERSION = '1.0.0';
const ACCENT = '#e54d42';
const LOW_VOLTAGE_THRESHOLD = 36.1;
const TOP_INSET = Platform.OS === 'android' ? (RNStatusBar.currentHeight ?? 0) : 0;

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

function formatDateTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function LoginScreen({ onLogin }: { onLogin: (user: User) => void }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);

  const submit = async () => {
    if (!username.trim() || !password) {
      Alert.alert('提示', '请输入用户名和密码');
      return;
    }
    setLoading(true);
    try {
      onLogin(await login(username.trim(), password));
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
        <Pressable style={styles.solidButton} onPress={submit} disabled={loading}>
          <Text style={styles.solidButtonText}>{loading ? '登录中...' : '登录'}</Text>
        </Pressable>
      </View>
    </SafeAreaView>
  );
}

function OrderCard({
  order,
  onDeliver,
}: {
  order: WaterOrder;
  onDeliver?: (order: WaterOrder) => void;
}) {
  const phone = dialablePhone(order.shelf_phone);
  return (
    <View style={styles.orderRow}>
      <Text style={styles.orderTime}>{formatDateTime(order.created_at)}</Text>
      <Text style={styles.orderAddress}>
        {order.address || `${order.station_name || ''} ${order.shelf_iccid}`.trim()}
      </Text>
      <Text style={styles.orderProduct}>
        {order.product_name || '未命名产品'}*{order.requested_quantity};
      </Text>
      <Text style={styles.orderContact}>微信 {order.shelf_wechat || '-'}</Text>
      {phone.dial ? (
        <Pressable onPress={() => callShelf(order.shelf_phone)} hitSlop={6}>
          <Text style={styles.orderPhone}>电话 {phone.display}</Text>
        </Pressable>
      ) : (
        <Text style={styles.orderContact}>电话 -</Text>
      )}
      <View style={styles.orderFooter}>
        <View style={styles.orderStatus}>
          <Text style={styles.orderMeta}>
            {order.status === 'pending'
              ? `触发余量 ${order.trigger_quantity}`
              : order.delivered_quantity != null
                ? `实送 ${order.delivered_quantity}`
                : order.status === 'delivered'
                  ? '已送达'
                  : '已取消'}
          </Text>
          <Text style={[
            styles.orderContact,
            Number(order.voltage) < LOW_VOLTAGE_THRESHOLD && styles.warningText,
          ]}>
            电压 {formatVoltage(order.voltage)}
          </Text>
        </View>
        {onDeliver ? (
          <Pressable style={styles.outlineButton} onPress={() => onDeliver(order)}>
            <Text style={styles.outlineButtonText}>确认送达</Text>
          </Pressable>
        ) : null}
      </View>
    </View>
  );
}

export default function App() {
  const [user, setUser] = useState<User | null>(null);
  const [restoring, setRestoring] = useState(true);
  const [tab, setTab] = useState<Tab>('pending');
  const [orders, setOrders] = useState<WaterOrder[]>([]);
  const [shelves, setShelves] = useState<Shelf[]>([]);
  const [loading, setLoading] = useState(false);
  const [deliveryOrder, setDeliveryOrder] = useState<WaterOrder | null>(null);
  const [deliveryQuantity, setDeliveryQuantity] = useState('');
  const [keyboardOffset, setKeyboardOffset] = useState(0);
  const [orderPage, setOrderPage] = useState(1);
  const [orderHasMore, setOrderHasMore] = useState(true);
  const [orderLoadingMore, setOrderLoadingMore] = useState(false);
  const [shelfSearchDraft, setShelfSearchDraft] = useState('');
  const [shelfSearchQuery, setShelfSearchQuery] = useState('');
  const [shelfPage, setShelfPage] = useState(1);
  const [shelfHasMore, setShelfHasMore] = useState(true);
  const [shelfLoadingMore, setShelfLoadingMore] = useState(false);
  const shelfSearchRequestId = useRef(0);
  const shelfLoadingMoreLock = useRef(false);
  const orderLoadingMoreLock = useRef(false);
  const deliveryQuantityInputRef = useRef<TextInput>(null);
  const appStateRef = useRef(AppState.currentState);

  const handleAuthExpired = useCallback(() => {
    setUser(null);
    setTab('pending');
    setOrders([]);
    setShelves([]);
    setDeliveryOrder(null);
  }, []);

  const handleRequestError = useCallback((error: unknown, fallbackTitle = '加载失败') => {
    if (error instanceof AuthExpiredError) {
      handleAuthExpired();
      return;
    }
    Alert.alert(fallbackTitle, error instanceof Error ? error.message : '请稍后重试');
  }, [handleAuthExpired]);

  useEffect(() => {
    let cancelled = false;
    restoreUser()
      .then(nextUser => {
        if (cancelled) return;
        setUser(nextUser);
        setTab('pending');
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
      if (prev.match(/inactive|background/) && nextState === 'active') {
        restoreUser().then(nextUser => {
          if (!nextUser) {
            handleAuthExpired();
            return;
          }
          setUser(nextUser);
        });
      }
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

  useEffect(() => {
    const draft = shelfSearchDraft.trim();
    if (draft === shelfSearchQuery) return;
    const timer = setTimeout(() => {
      setShelfSearchQuery(draft);
    }, SHELF_SEARCH_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [shelfSearchDraft, shelfSearchQuery]);

  const loadShelves = useCallback(async (page = 1, append = false) => {
    if (!user) return;
    const requestId = ++shelfSearchRequestId.current;
    if (append) {
      if (shelfLoadingMoreLock.current || !shelfHasMore) return;
      shelfLoadingMoreLock.current = true;
      setShelfLoadingMore(true);
    } else {
      setLoading(true);
      setShelfHasMore(true);
    }
    try {
      const nextShelves = await getShelves(shelfSearchQuery, page, SHELF_PAGE_SIZE);
      if (requestId !== shelfSearchRequestId.current) return;
      setShelves(prev => {
        if (!append) return nextShelves;
        const seen = new Set(prev.map(item => item.id));
        return [...prev, ...nextShelves.filter(item => !seen.has(item.id))];
      });
      setShelfPage(page);
      setShelfHasMore(nextShelves.length >= SHELF_PAGE_SIZE);
    } catch (error) {
      if (requestId !== shelfSearchRequestId.current) return;
      handleRequestError(error);
    } finally {
      if (requestId === shelfSearchRequestId.current) {
        setLoading(false);
        setShelfLoadingMore(false);
        shelfLoadingMoreLock.current = false;
      }
    }
  }, [user, shelfSearchQuery, shelfHasMore, handleRequestError]);

  const loadOrders = useCallback(async (page = 1, append = false) => {
    if (!user || (tab !== 'pending' && tab !== 'history')) return;
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
      const result = await getOrders(tab, page, PAGE_SIZE);
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
  }, [tab, user, orderHasMore, handleRequestError]);

  useEffect(() => {
    if (!user || tab === 'more') return;
    if (tab === 'shelves') {
      loadShelves(1, false);
      return;
    }
    if (tab === 'pending' || tab === 'history') {
      loadOrders(1, false);
    }
  }, [user, tab, shelfSearchQuery]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadMoreShelves = () => {
    if (tab !== 'shelves' || loading || shelfLoadingMore || !shelfHasMore) return;
    loadShelves(shelfPage + 1, true);
  };

  const loadMoreOrders = () => {
    if ((tab !== 'pending' && tab !== 'history') || loading || orderLoadingMore || !orderHasMore) return;
    loadOrders(orderPage + 1, true);
  };

  const switchTab = (next: Tab) => {
    setTab(next);
  };

  const applyShelfSearchNow = (value?: string) => {
    const next = (value ?? shelfSearchDraft).trim();
    setShelfSearchDraft(next);
    setShelfSearchQuery(next);
    Keyboard.dismiss();
  };

  const clearShelfSearch = () => {
    setShelfSearchDraft('');
    setShelfSearchQuery('');
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
      await deliverOrder(deliveryOrder.id, quantity);
      setDeliveryOrder(null);
      setDeliveryQuantity('');
      Alert.alert('完成', '订单已送达，货架余量已增加');
      loadOrders(1, false);
    } catch (error) {
      handleRequestError(error, '操作失败');
    }
  };

  const handleShelfOrder = async (shelf: Shelf) => {
    try {
      const active = await getActiveOrder(shelf.id);
      if (active) {
        setTab('pending');
        Alert.alert('已有订单', `订单 ${active.order_no} 已在待配送列表中`);
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
          setUser(nextUser);
          setTab('pending');
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
          ['shelves', '货架'],
          ['history', '历史订单'],
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
            <Text style={styles.moreMeta}>版本号：v{APP_VERSION}</Text>
            <Text style={styles.moreMeta}>版权所有 ©搬夫科技</Text>
          </View>
          <Pressable style={styles.logoutButton} onPress={confirmLogout}>
            <Text style={styles.solidButtonText}>注销</Text>
          </Pressable>
        </View>
      ) : tab === 'shelves' ? (
        <View style={styles.shelvesPane}>
          <View style={styles.searchPanel}>
            <View style={styles.searchRow}>
              <TextInput
                style={styles.searchInput}
                placeholder="设备号、电话、微信、地址、备注"
                placeholderTextColor="#999"
                value={shelfSearchDraft}
                onChangeText={setShelfSearchDraft}
                returnKeyType="search"
                autoCorrect={false}
                autoCapitalize="none"
                clearButtonMode="never"
                onSubmitEditing={() => applyShelfSearchNow()}
              />
              {shelfSearchDraft.length > 0 ? (
                <Pressable style={styles.searchClear} onPress={clearShelfSearch} hitSlop={8}>
                  <Text style={styles.searchClearText}>清除</Text>
                </Pressable>
              ) : null}
              <Pressable style={styles.searchButton} onPress={() => applyShelfSearchNow()}>
                <Text style={styles.searchButtonText}>搜索</Text>
              </Pressable>
            </View>
            <Text style={styles.searchHint}>
              {shelfSearchQuery
                ? loading && shelves.length === 0
                  ? `正在搜索「${shelfSearchQuery}」…`
                  : `「${shelfSearchQuery}」已加载 ${shelves.length} 条${shelfHasMore ? '，继续下滑加载更多' : ''}`
                : shelfHasMore
                  ? `已加载 ${shelves.length} 条，下滑继续加载`
                  : `共 ${shelves.length} 条货架`}
            </Text>
          </View>
          <FlatList
            data={shelves}
            keyExtractor={item => String(item.id)}
            keyboardShouldPersistTaps="handled"
            refreshControl={
              <RefreshControl
                refreshing={loading && shelves.length === 0}
                onRefresh={() => loadShelves(1, false)}
                colors={[ACCENT]}
                tintColor={ACCENT}
              />
            }
            contentContainerStyle={styles.list}
            onEndReached={loadMoreShelves}
            onEndReachedThreshold={0.3}
            ListEmptyComponent={
              !loading ? (
                <Text style={styles.empty}>
                  {shelfSearchQuery ? '未找到匹配的货架，试试换个关键词' : '暂无货架'}
                </Text>
              ) : (
                <View style={styles.listLoading}>
                  <ActivityIndicator color={ACCENT} />
                </View>
              )
            }
            ListFooterComponent={
              shelfLoadingMore ? (
                <View style={styles.listLoading}>
                  <ActivityIndicator color={ACCENT} />
                  <Text style={styles.muted}>加载更多…</Text>
                </View>
              ) : shelves.length > 0 && !shelfHasMore ? (
                <Text style={styles.listEnd}>已经到底了</Text>
              ) : null
            }
            renderItem={({ item }) => (
              <View style={styles.orderRow}>
                <Text style={styles.orderAddress}>{item.product_name || '未命名产品'} · {item.iccid}</Text>
                {(item.station_name || item.city_name) ? (
                  <Text style={styles.orderProduct}>
                    {[item.city_name, item.station_name].filter(Boolean).join(' · ')}
                  </Text>
                ) : null}
                <Text style={styles.orderProduct}>{item.address || '暂无地址'}</Text>
                <View style={styles.orderFooter}>
                  <Text style={[styles.orderMeta, item.current_quantity <= item.warning_quantity && styles.warningText]}>
                    余量 {item.current_quantity} / 预警 {item.warning_quantity}
                  </Text>
                  <Pressable style={styles.outlineButton} onPress={() => handleShelfOrder(item)}>
                    <Text style={styles.outlineButtonText}>生成订单</Text>
                  </Pressable>
                </View>
              </View>
            )}
          />
        </View>
      ) : (
        <FlatList
          data={orders}
          keyExtractor={item => String(item.id)}
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
              <Text style={styles.empty}>暂无订单</Text>
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
          renderItem={({ item }) => (
            <OrderCard
              order={item}
              onDeliver={tab === 'pending' ? order => {
                setDeliveryOrder(order);
                setDeliveryQuantity(String(order.requested_quantity));
              } : undefined}
            />
          )}
        />
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
              <Text style={styles.orderContact}>微信 {deliveryOrder?.shelf_wechat || '-'}</Text>
              {dialablePhone(deliveryOrder?.shelf_phone).dial ? (
                <Pressable onPress={() => callShelf(deliveryOrder?.shelf_phone)} hitSlop={6}>
                  <Text style={styles.orderPhone}>电话 {dialablePhone(deliveryOrder?.shelf_phone).display}</Text>
                </Pressable>
              ) : (
                <Text style={styles.orderContact}>电话 -</Text>
              )}
              <Text style={[
                styles.orderContact,
                Number(deliveryOrder?.voltage) < LOW_VOLTAGE_THRESHOLD && styles.warningText,
              ]}>
                电压 {formatVoltage(deliveryOrder?.voltage)}
              </Text>
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
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  page: { flex: 1, backgroundColor: '#fff' },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', backgroundColor: '#fff' },
  loginPage: { flex: 1, justifyContent: 'center', padding: 24, backgroundColor: '#fff' },
  loginCard: { gap: 14 },
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
  tabText: { color: '#666', fontSize: 15, fontWeight: '500', paddingBottom: 10 },
  activeTabText: { color: ACCENT, fontWeight: '700' },
  tabUnderline: { alignSelf: 'stretch', height: 2, backgroundColor: 'transparent' },
  activeTabUnderline: { backgroundColor: ACCENT },
  shelvesPane: { flex: 1 },
  searchPanel: {
    backgroundColor: '#fff',
    paddingHorizontal: 12,
    paddingTop: 10,
    paddingBottom: 8,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#e5e5e5',
    gap: 6,
  },
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
  orderTime: { color: '#999', fontSize: 13 },
  orderAddress: { color: '#222', fontSize: 16, fontWeight: '700', lineHeight: 22 },
  orderProduct: { color: '#333', fontSize: 15, lineHeight: 21 },
  orderContact: { color: '#666', fontSize: 13, lineHeight: 18 },
  orderPhone: { color: ACCENT, fontSize: 14, fontWeight: '700', lineHeight: 20 },
  orderStatus: { flex: 1, gap: 2 },
  orderFooter: {
    marginTop: 4,
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: 12,
  },
  orderMeta: { color: ACCENT, fontSize: 13, flex: 1 },
  warningText: { color: ACCENT, fontWeight: '700' },
  outlineButton: {
    borderWidth: 1,
    borderColor: ACCENT,
    borderRadius: 6,
    paddingHorizontal: 16,
    paddingVertical: 7,
    minWidth: 88,
    alignItems: 'center',
  },
  outlineButtonText: { color: ACCENT, fontWeight: '700', fontSize: 14 },
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
    paddingVertical: 11,
    backgroundColor: '#fff',
    fontSize: 16,
  },
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
