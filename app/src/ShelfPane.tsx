import DateTimePicker, { DateTimePickerEvent } from '@react-native-community/datetimepicker';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  ActivityIndicator,
  Alert,
  FlatList,
  Keyboard,
  Linking,
  Modal,
  Platform,
  Pressable,
  RefreshControl,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
  type ViewStyle,
} from 'react-native';
import {
  createShelf,
  deleteShelf,
  getCities,
  getShelfQuantityLogs,
  getShelfShippingLogs,
  getShelves,
  getStations,
  shipShelf,
  updateShelf,
} from './api';
import type { CityOption, Shelf, ShelfListQuery, StationOption } from './types';

const ACCENT = '#2563eb';
const DANGER = '#e54d42';
const PAGE_SIZE = 10;
const LOW_VOLTAGE = 36.1;

type DeviceType = '' | 'shelf' | 'tea_bar';
type OnlineFilter = '' | '0' | '1';
type DeliveryFilter = '' | '1' | '2';

type ShelfFilters = {
  cityId: number | null;
  stationId: number | null;
  deviceType: DeviceType;
  onlineStatus: OnlineFilter;
  deliveryStatus: DeliveryFilter;
  paymentMethod: string;
  minQuantity: string;
  maxQuantity: string;
  lowStock: boolean;
  lowVoltage: boolean;
  simExpiry: boolean;
  lowSignal: boolean;
};

const EMPTY_FILTERS: ShelfFilters = {
  cityId: null,
  stationId: null,
  deviceType: '',
  onlineStatus: '',
  deliveryStatus: '',
  paymentMethod: '',
  minQuantity: '',
  maxQuantity: '',
  lowStock: false,
  lowVoltage: false,
  simExpiry: false,
  lowSignal: false,
};

type ShelfForm = {
  id?: number;
  iccid: string;
  device_type: 'shelf' | 'tea_bar';
  product_name: string;
  cityId: number | null;
  stationId: number | null;
  wechat: string;
  phone: string;
  address: string;
  total_quantity: string;
  order_quantity: string;
  warning_quantity: string;
  current_quantity: string;
  sim_card_number: string;
  sim_card_expiry: string;
  payment_method: string;
  remark: string;
  voltage: string;
  signal_strength: string;
  version: string;
};

function emptyForm(): ShelfForm {
  return {
    iccid: '',
    device_type: 'shelf',
    product_name: '',
    cityId: null,
    stationId: null,
    wechat: '',
    phone: '',
    address: '',
    total_quantity: '',
    order_quantity: '',
    warning_quantity: '3',
    current_quantity: '0',
    sim_card_number: '',
    sim_card_expiry: '',
    payment_method: '',
    remark: '',
    voltage: '0',
    signal_strength: '0',
    version: '',
  };
}

function pad(n: number) {
  return String(n).padStart(2, '0');
}

function formatDateTime(value?: string | null) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function formatDate(value?: string | null) {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value.slice(0, 10);
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

function dialablePhone(phone?: string | null) {
  const display = (phone || '').trim();
  const href = display.replace(/[^\d+]/g, '');
  return { display: display || '-', href };
}

function deviceLabel(type?: string | null) {
  return type === 'tea_bar' ? '茶吧机' : '货架';
}

function countFilters(filters: ShelfFilters) {
  let count = 0;
  if (filters.cityId) count += 1;
  if (filters.stationId) count += 1;
  if (filters.deviceType) count += 1;
  if (filters.onlineStatus) count += 1;
  if (filters.deliveryStatus) count += 1;
  if (filters.paymentMethod) count += 1;
  if (filters.minQuantity.trim() || filters.maxQuantity.trim()) count += 1;
  if (filters.lowStock) count += 1;
  if (filters.lowVoltage) count += 1;
  if (filters.simExpiry) count += 1;
  if (filters.lowSignal) count += 1;
  return count;
}

function FormField({
  label,
  required = false,
  children,
  style,
}: {
  label: string;
  required?: boolean;
  children: React.ReactNode;
  style?: ViewStyle;
}) {
  return (
    <View style={[styles.formField, style]}>
      <Text style={styles.formLabel}>
        {label}
        {required ? <Text style={styles.requiredMark}> *</Text> : null}
      </Text>
      {children}
    </View>
  );
}

function Chip({
  label,
  active,
  onPress,
}: {
  label: string;
  active?: boolean;
  onPress: () => void;
}) {
  return (
    <Pressable style={[styles.chip, active && styles.chipActive]} onPress={onPress}>
      <Text style={[styles.chipText, active && styles.chipTextActive]}>{label}</Text>
    </Pressable>
  );
}

export function ShelfPane({
  username,
  onGenerateOrder,
  onError,
}: {
  username: string;
  onGenerateOrder: (shelf: Shelf) => void;
  onError: (error: unknown, title?: string) => void;
}) {
  const [shelves, setShelves] = useState<Shelf[]>([]);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const [searchDraft, setSearchDraft] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [draftFilters, setDraftFilters] = useState<ShelfFilters>(EMPTY_FILTERS);
  const [appliedFilters, setAppliedFilters] = useState<ShelfFilters>(EMPTY_FILTERS);
  const [cities, setCities] = useState<CityOption[]>([]);
  const [stations, setStations] = useState<StationOption[]>([]);
  const [formStations, setFormStations] = useState<StationOption[]>([]);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [picker, setPicker] = useState<'filterCity' | 'filterStation' | 'formCity' | 'formStation' | null>(null);
  const [form, setForm] = useState<ShelfForm | null>(null);
  const [expiryPicker, setExpiryPicker] = useState(false);
  const [logTitle, setLogTitle] = useState('');
  const [logLines, setLogLines] = useState<string[] | null>(null);
  const [teaBarShelf, setTeaBarShelf] = useState<Shelf | null>(null);
  const [teaBarQty, setTeaBarQty] = useState('');
  const requestIdRef = useRef(0);
  const loadingMoreLock = useRef(false);

  const load = useCallback(async (
    nextPage = 1,
    append = false,
    search = searchQuery,
    filters = appliedFilters,
  ) => {
    const requestId = ++requestIdRef.current;
    if (append) {
      if (loadingMoreLock.current || !hasMore) return;
      loadingMoreLock.current = true;
      setLoadingMore(true);
    } else {
      setLoading(true);
      setHasMore(true);
    }
    const query: ShelfListQuery = {
      search,
      cityId: filters.cityId,
      stationId: filters.stationId,
      deviceType: filters.deviceType,
      onlineStatus: filters.onlineStatus,
      deliveryStatus: filters.deliveryStatus,
      paymentMethod: filters.paymentMethod,
      minQuantity: filters.minQuantity,
      maxQuantity: filters.maxQuantity,
      lowStock: filters.lowStock,
      lowVoltage: filters.lowVoltage,
      simExpiry: filters.simExpiry,
      lowSignal: filters.lowSignal,
      page: nextPage,
      pageSize: PAGE_SIZE,
    };
    try {
      const next = await getShelves(query);
      if (requestId !== requestIdRef.current) return;
      setShelves(prev => {
        if (!append) return next;
        const seen = new Set(prev.map(item => item.id));
        return [...prev, ...next.filter(item => !seen.has(item.id))];
      });
      setPage(nextPage);
      setHasMore(next.length >= PAGE_SIZE);
    } catch (error) {
      if (requestId !== requestIdRef.current) return;
      onError(error);
    } finally {
      if (requestId === requestIdRef.current) {
        setLoading(false);
        setLoadingMore(false);
        loadingMoreLock.current = false;
      }
    }
  }, [appliedFilters, hasMore, onError, searchQuery]);

  useEffect(() => {
    load(1, false);
  }, [searchQuery, appliedFilters]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    getCities().then(setCities).catch(error => onError(error, '城市加载失败'));
  }, [onError]);

  useEffect(() => {
    if (!draftFilters.cityId) {
      setStations([]);
      return;
    }
    getStations(draftFilters.cityId).then(setStations).catch(error => onError(error, '站点加载失败'));
  }, [draftFilters.cityId, onError]);

  useEffect(() => {
    if (!form?.cityId) {
      setFormStations([]);
      return;
    }
    getStations(form.cityId).then(setFormStations).catch(error => onError(error, '站点加载失败'));
  }, [form?.cityId, onError]);

  const applySearch = () => {
    setSearchQuery(searchDraft.trim());
    Keyboard.dismiss();
  };

  const applyFilters = () => {
    setAppliedFilters(draftFilters);
    setFiltersOpen(false);
    Keyboard.dismiss();
  };

  const clearFilters = () => {
    setDraftFilters(EMPTY_FILTERS);
    setAppliedFilters(EMPTY_FILTERS);
    setFiltersOpen(false);
  };

  const toggleExpand = (id: number) => {
    setExpandedId(current => (current === id ? null : id));
  };

  const callPhone = (phone?: string | null) => {
    const dial = dialablePhone(phone);
    if (!dial.href) return;
    Linking.openURL(`tel:${dial.href}`).catch(() => Alert.alert('无法拨打', '请检查电话权限'));
  };

  const openCreate = () => {
    setForm(emptyForm());
  };

  const openEdit = (shelf: Shelf) => {
    const city = cities.find(item => item.city_name === shelf.city_name);
    setForm({
      id: shelf.id,
      iccid: shelf.iccid,
      device_type: shelf.device_type === 'tea_bar' ? 'tea_bar' : 'shelf',
      product_name: shelf.product_name || '',
      cityId: city?.id ?? null,
      stationId: shelf.station_id ?? null,
      wechat: shelf.wechat || '',
      phone: shelf.phone || '',
      address: shelf.address || '',
      total_quantity: String(shelf.total_quantity ?? ''),
      order_quantity: String(shelf.order_quantity ?? ''),
      warning_quantity: String(shelf.warning_quantity ?? 3),
      current_quantity: String(shelf.current_quantity ?? 0),
      sim_card_number: shelf.sim_card_number || '',
      sim_card_expiry: formatDate(shelf.sim_card_expiry),
      payment_method: shelf.payment_method || '',
      remark: shelf.remark || '',
      voltage: String(shelf.voltage ?? 0),
      signal_strength: String(shelf.signal_strength ?? 0),
      version: shelf.version || '',
    });
  };

  const saveForm = async () => {
    if (!form) return;
    const required: Array<[string, string]> = [
      [form.iccid.trim(), '请填写设备号'],
      [form.product_name.trim(), '请填写产品名称'],
      [form.stationId ? '1' : '', '请选择站点'],
      [form.wechat.trim(), '请填写微信'],
      [form.phone.trim(), '请填写电话'],
      [form.address.trim(), '请填写地址'],
      [form.sim_card_number.trim(), '请填写流量卡号'],
      [form.sim_card_expiry.trim(), '请选择流量卡到期日'],
    ];
    const missing = required.find(([value]) => !value);
    if (missing) {
      Alert.alert('提示', missing[1]);
      return;
    }
    const total = Number(form.total_quantity);
    const orderQty = Number(form.order_quantity);
    const warning = Number(form.warning_quantity);
    const current = Number(form.current_quantity || 0);
    if (![total, orderQty, warning, current].every(value => Number.isInteger(value) && value >= 0)) {
      Alert.alert('提示', '数量请填写大于等于 0 的整数');
      return;
    }
    const payload = {
      iccid: form.iccid.trim(),
      wechat: form.wechat.trim(),
      phone: form.phone.trim(),
      address: form.address.trim(),
      total_quantity: total,
      product_name: form.product_name.trim(),
      order_quantity: orderQty,
      current_quantity: current,
      warning_quantity: warning,
      sim_card_number: form.sim_card_number.trim(),
      sim_card_expiry: `${form.sim_card_expiry}T00:00:00`,
      station_id: form.stationId as number,
      device_type: form.device_type,
      payment_method: form.payment_method.trim(),
      remark: form.remark.trim(),
      voltage: Number(form.voltage || 0),
      signal_strength: Number(form.signal_strength || 0),
      version: form.version.trim(),
      created_by: username,
      updated_by: username,
    };
    try {
      if (form.id) await updateShelf(form.id, payload);
      else await createShelf(payload);
      setForm(null);
      load(1, false);
    } catch (error) {
      onError(error, '保存失败');
    }
  };

  const confirmDelete = (shelf: Shelf) => {
    Alert.alert('删除设备', `确认删除 ${shelf.iccid}？`, [
      { text: '取消', style: 'cancel' },
      {
        text: '删除',
        style: 'destructive',
        onPress: async () => {
          try {
            await deleteShelf(shelf.id);
            load(1, false);
          } catch (error) {
            onError(error, '删除失败');
          }
        },
      },
    ]);
  };

  const confirmShip = (shelf: Shelf) => {
    Alert.alert('发货', `确认将 ${shelf.iccid} 标为已发货？`, [
      { text: '取消', style: 'cancel' },
      {
        text: '发货',
        onPress: async () => {
          try {
            await shipShelf(shelf.id);
            load(1, false);
          } catch (error) {
            onError(error, '发货失败');
          }
        },
      },
    ]);
  };

  const openQuantityLogs = async (shelf: Shelf) => {
    try {
      const logs = await getShelfQuantityLogs(shelf.id);
      setLogTitle(`${shelf.iccid} 余量日志`);
      setLogLines(logs.length
        ? logs.map(item => `${formatDateTime(item.log_time)}  余量 ${item.current_quantity}`)
        : ['暂无日志']);
    } catch (error) {
      onError(error, '日志加载失败');
    }
  };

  const openShippingLogs = async (shelf: Shelf) => {
    try {
      const result = await getShelfShippingLogs(shelf.id);
      setLogTitle(`${shelf.iccid} 发货记录`);
      setLogLines(result.logs.length
        ? result.logs.map(item => formatDateTime(item.ship_time))
        : ['暂无发货记录']);
    } catch (error) {
      onError(error, '发货记录加载失败');
    }
  };

  const saveTeaBarQty = async () => {
    if (!teaBarShelf) return;
    const quantity = Number(teaBarQty);
    if (!Number.isInteger(quantity) || quantity < 0) {
      Alert.alert('提示', '请输入大于等于 0 的整数');
      return;
    }
    try {
      await updateShelf(teaBarShelf.id, {
        current_quantity: quantity,
        device_type: 'tea_bar',
        updated_by: username,
      });
      setTeaBarShelf(null);
      load(1, false);
    } catch (error) {
      onError(error, '保存失败');
    }
  };

  const cityName = (id: number | null, list = cities) => list.find(item => item.id === id)?.city_name || '全部城市';
  const stationName = (id: number | null, list: StationOption[], emptyLabel: string) => (
    list.find(item => item.id === id)?.station_name || emptyLabel
  );
  const filterCount = countFilters(appliedFilters);
  const pickerOptions = picker === 'filterCity' || picker === 'formCity'
    ? cities.map(item => ({ id: item.id, label: item.city_name }))
    : (picker === 'formStation' ? formStations : stations).map(item => ({ id: item.id, label: item.station_name }));

  const choosePicker = (id: number) => {
    if (picker === 'filterCity') {
      setDraftFilters(current => ({ ...current, cityId: id, stationId: null }));
    } else if (picker === 'filterStation') {
      setDraftFilters(current => ({ ...current, stationId: id }));
    } else if (picker === 'formCity') {
      setForm(current => current ? { ...current, cityId: id, stationId: null } : current);
    } else if (picker === 'formStation') {
      setForm(current => current ? { ...current, stationId: id } : current);
    }
    setPicker(null);
  };

  const onExpiryPicked = (event: DateTimePickerEvent, selected?: Date) => {
    if (Platform.OS === 'android') setExpiryPicker(false);
    if (event.type === 'dismissed' || !selected) return;
    setForm(current => current ? {
      ...current,
      sim_card_expiry: `${selected.getFullYear()}-${pad(selected.getMonth() + 1)}-${pad(selected.getDate())}`,
    } : current);
  };

  return (
    <View style={styles.pane}>
      <View style={styles.searchPanel}>
        <View style={styles.searchRow}>
          <TextInput
            style={styles.searchInput}
            placeholder="设备号、电话、微信、地址、备注"
            placeholderTextColor="#999"
            value={searchDraft}
            onChangeText={setSearchDraft}
            returnKeyType="search"
            onSubmitEditing={applySearch}
          />
          {searchDraft.length > 0 ? (
            <Pressable onPress={() => { setSearchDraft(''); setSearchQuery(''); }} hitSlop={8}>
              <Text style={styles.clearText}>清除</Text>
            </Pressable>
          ) : null}
          <Pressable style={styles.searchButton} onPress={applySearch}>
            <Text style={styles.searchButtonText}>搜索</Text>
          </Pressable>
          <Pressable style={[styles.filterButton, filterCount > 0 && styles.filterButtonActive]} onPress={() => setFiltersOpen(open => !open)}>
            <Text style={[styles.filterButtonText, filterCount > 0 && styles.filterButtonTextActive]}>
              {filterCount > 0 ? `筛选 ${filterCount}` : '筛选'}
            </Text>
          </Pressable>
        </View>
        {filtersOpen ? (
          <View style={styles.filterBox}>
            <View style={styles.fieldRow}>
              <Pressable style={styles.select} onPress={() => setPicker('filterCity')}>
                <Text style={styles.selectText} numberOfLines={1}>{draftFilters.cityId ? cityName(draftFilters.cityId) : '全部城市'}</Text>
              </Pressable>
              <Pressable
                style={[styles.select, !draftFilters.cityId && styles.selectDisabled]}
                onPress={() => draftFilters.cityId && setPicker('filterStation')}
              >
                <Text style={styles.selectText} numberOfLines={1}>
                  {draftFilters.stationId ? stationName(draftFilters.stationId, stations, '全部站点') : '全部站点'}
                </Text>
              </Pressable>
            </View>
            <Text style={styles.fieldLabel}>设备类型</Text>
            <View style={styles.chipRow}>
              {([['', '全部'], ['shelf', '货架'], ['tea_bar', '茶吧机']] as Array<[DeviceType, string]>).map(([value, label]) => (
                <Chip key={label} label={label} active={draftFilters.deviceType === value} onPress={() => setDraftFilters(current => ({ ...current, deviceType: value }))} />
              ))}
            </View>
            <Text style={styles.fieldLabel}>在线</Text>
            <View style={styles.chipRow}>
              {([['', '全部'], ['1', '在线'], ['0', '离线']] as Array<[OnlineFilter, string]>).map(([value, label]) => (
                <Chip key={`online-${label}`} label={label} active={draftFilters.onlineStatus === value} onPress={() => setDraftFilters(current => ({ ...current, onlineStatus: value }))} />
              ))}
            </View>
            <Text style={styles.fieldLabel}>发货</Text>
            <View style={styles.chipRow}>
              {([['', '全部'], ['1', '发货中'], ['2', '非发货中']] as Array<[DeliveryFilter, string]>).map(([value, label]) => (
                <Chip key={`ship-${label}`} label={label} active={draftFilters.deliveryStatus === value} onPress={() => setDraftFilters(current => ({ ...current, deliveryStatus: value }))} />
              ))}
            </View>
            <Text style={styles.fieldLabel}>支付</Text>
            <View style={styles.chipRow}>
              {['', '未支付', '已联系'].map(value => (
                <Chip key={`pay-${value || 'all'}`} label={value || '全部'} active={draftFilters.paymentMethod === value} onPress={() => setDraftFilters(current => ({ ...current, paymentMethod: value }))} />
              ))}
            </View>
            <Text style={styles.fieldLabel}>余量区间</Text>
            <View style={styles.fieldRow}>
              <TextInput style={styles.rangeInput} keyboardType="number-pad" placeholder="最少" placeholderTextColor="#999" value={draftFilters.minQuantity} onChangeText={value => setDraftFilters(current => ({ ...current, minQuantity: value }))} />
              <Text style={styles.rangeDash}>-</Text>
              <TextInput style={styles.rangeInput} keyboardType="number-pad" placeholder="最大" placeholderTextColor="#999" value={draftFilters.maxQuantity} onChangeText={value => setDraftFilters(current => ({ ...current, maxQuantity: value }))} />
            </View>
            <View style={styles.chipRow}>
              <Chip label="低库存" active={draftFilters.lowStock} onPress={() => setDraftFilters(current => ({ ...current, lowStock: !current.lowStock }))} />
              <Chip label="低电压" active={draftFilters.lowVoltage} onPress={() => setDraftFilters(current => ({ ...current, lowVoltage: !current.lowVoltage }))} />
              <Chip label="流量卡到期" active={draftFilters.simExpiry} onPress={() => setDraftFilters(current => ({ ...current, simExpiry: !current.simExpiry }))} />
              <Chip label="信号弱" active={draftFilters.lowSignal} onPress={() => setDraftFilters(current => ({ ...current, lowSignal: !current.lowSignal }))} />
            </View>
            <View style={styles.filterActions}>
              <Pressable style={styles.ghostButton} onPress={clearFilters}>
                <Text style={styles.ghostButtonText}>清空</Text>
              </Pressable>
              <Pressable style={styles.primaryButton} onPress={applyFilters}>
                <Text style={styles.primaryButtonText}>查询</Text>
              </Pressable>
            </View>
          </View>
        ) : null}
        <View style={styles.hintRow}>
          <Text style={styles.hint}>
            {loading && shelves.length === 0 ? '加载中…' : `已加载 ${shelves.length} 条${hasMore ? '，下滑继续' : ''}`}
          </Text>
          <Pressable onPress={openCreate}>
            <Text style={styles.addText}>添加设备</Text>
          </Pressable>
        </View>
      </View>
      <FlatList
        data={shelves}
        keyExtractor={item => String(item.id)}
        keyboardShouldPersistTaps="handled"
        refreshControl={<RefreshControl refreshing={loading && shelves.length === 0} onRefresh={() => load(1, false)} colors={[ACCENT]} tintColor={ACCENT} />}
        contentContainerStyle={styles.list}
        onEndReached={() => {
          if (!loading && !loadingMore && hasMore) load(page + 1, true);
        }}
        onEndReachedThreshold={0.3}
        ListEmptyComponent={!loading ? <Text style={styles.empty}>暂无货架</Text> : null}
        ListFooterComponent={loadingMore ? <ActivityIndicator color={ACCENT} style={styles.footer} /> : null}
        renderItem={({ item, index }) => {
          const expanded = expandedId === item.id;
          const lowStock = item.current_quantity <= item.warning_quantity;
          const lowVoltage = Number(item.voltage) < LOW_VOLTAGE;
          const phone = dialablePhone(item.phone);
          return (
            <Pressable style={[styles.card, index % 2 === 1 && styles.cardAlt]} onPress={() => toggleExpand(item.id)}>
              <View style={styles.titleRow}>
                <Text style={styles.title} numberOfLines={1}>{item.product_name || '未命名产品'} · {item.iccid}</Text>
                <Text style={[styles.tag, item.device_type === 'tea_bar' && styles.tagTea]}>{deviceLabel(item.device_type)}</Text>
              </View>
              <Text style={styles.address} numberOfLines={1}>{item.address || '暂无地址'}</Text>
              <View style={styles.summaryRow}>
                <Text style={[styles.summary, lowStock && styles.danger]}>余量 {item.current_quantity}/{item.total_quantity}</Text>
                <Text style={styles.summary}>{item.online_status === 1 ? '在线' : '离线'}</Text>
                <Text style={[styles.summary, lowVoltage && styles.danger]}>{Number(item.voltage ?? 0).toFixed(1)}V</Text>
                <Pressable style={styles.miniButton} onPress={() => callPhone(item.phone)}>
                  <Text style={styles.miniButtonText}>{phone.href ? '电话' : '无电话'}</Text>
                </Pressable>
                <Pressable style={styles.miniButton} onPress={() => onGenerateOrder(item)}>
                  <Text style={styles.miniButtonText}>生成订单</Text>
                </Pressable>
              </View>
              {expanded ? (
                <View style={styles.detail}>
                  <Text style={styles.detailLine}>微信 {item.wechat || '-'}</Text>
                  <Text style={styles.detailLine}>电话 {phone.display}</Text>
                  <Text style={styles.detailLine}>站点 {[item.city_name, item.station_name].filter(Boolean).join(' · ') || '-'}</Text>
                  <Text style={styles.detailLine}>预警 {item.warning_quantity} · 订购 {item.order_quantity}</Text>
                  <Text style={styles.detailLine}>发货 {item.delivery_status === 1 ? '已发货' : '未发货'} · 支付 {item.payment_method || '-'}</Text>
                  <Text style={styles.detailLine}>信号 {item.signal_strength ?? '-'} · 流量卡 {item.sim_card_number || '-'}</Text>
                  <Text style={styles.detailLine}>到期 {formatDate(item.sim_card_expiry) || '-'} · 更新 {formatDateTime(item.push_time)}</Text>
                  <Text style={styles.detailLine}>备注 {item.remark?.trim() || '无'}</Text>
                  <View style={styles.actionRow}>
                    {item.delivery_status !== 1 ? (
                      <Pressable onPress={() => confirmShip(item)}><Text style={styles.actionText}>发货</Text></Pressable>
                    ) : null}
                    <Pressable onPress={() => openQuantityLogs(item)}><Text style={styles.actionText}>日志</Text></Pressable>
                    <Pressable onPress={() => openShippingLogs(item)}><Text style={styles.actionText}>发货记录</Text></Pressable>
                    {item.device_type === 'tea_bar' ? (
                      <Pressable onPress={() => { setTeaBarShelf(item); setTeaBarQty(String(item.current_quantity)); }}>
                        <Text style={styles.actionText}>设库存</Text>
                      </Pressable>
                    ) : null}
                  </View>
                  <View style={styles.actionRow}>
                    <Pressable onPress={() => openEdit(item)}><Text style={styles.actionText}>编辑</Text></Pressable>
                    <Pressable onPress={() => confirmDelete(item)}><Text style={styles.dangerAction}>删除</Text></Pressable>
                  </View>
                </View>
              ) : null}
            </Pressable>
          );
        }}
      />

      <Modal visible={!!picker} transparent animationType="fade" onRequestClose={() => setPicker(null)}>
        <Pressable style={styles.backdrop} onPress={() => setPicker(null)}>
          <View style={styles.sheet}>
            <FlatList
              data={picker === 'filterCity' ? [{ id: 0, label: '全部城市' }, ...pickerOptions] : pickerOptions}
              keyExtractor={item => String(item.id)}
              renderItem={({ item }) => (
                <Pressable style={styles.option} onPress={() => (item.id === 0 ? (setDraftFilters(current => ({ ...current, cityId: null, stationId: null })), setPicker(null)) : choosePicker(item.id))}>
                  <Text style={styles.optionText}>{item.label}</Text>
                </Pressable>
              )}
              ListEmptyComponent={<Text style={styles.empty}>暂无选项</Text>}
            />
          </View>
        </Pressable>
      </Modal>

      <Modal visible={!!form} animationType="slide" onRequestClose={() => setForm(null)}>
        <View style={styles.formPage}>
          <View style={styles.formHeader}>
            <Pressable onPress={() => setForm(null)}><Text style={styles.clearText}>关闭</Text></Pressable>
            <Text style={styles.formTitle}>{form?.id ? '编辑设备' : '添加设备'}</Text>
            <Pressable onPress={saveForm}><Text style={styles.addText}>保存</Text></Pressable>
          </View>
          <ScrollView contentContainerStyle={styles.formBody} keyboardShouldPersistTaps="handled">
            <FormField label="设备号 (ICCID)" required>
              <TextInput style={styles.input} placeholder="请输入设备号" placeholderTextColor="#999" value={form?.iccid} onChangeText={value => setForm(current => current ? { ...current, iccid: value } : current)} />
            </FormField>
            <FormField label="设备类型" required>
              <View style={styles.chipRow}>
                <Chip label="货架" active={form?.device_type === 'shelf'} onPress={() => setForm(current => current ? { ...current, device_type: 'shelf' } : current)} />
                <Chip label="茶吧机" active={form?.device_type === 'tea_bar'} onPress={() => setForm(current => current ? { ...current, device_type: 'tea_bar' } : current)} />
              </View>
            </FormField>
            <View style={styles.formPair}>
              <FormField label="所在城市" style={styles.formHalf}>
                <Pressable style={styles.formSelect} onPress={() => setPicker('formCity')}>
                  <Text style={form?.cityId ? styles.selectText : styles.selectPlaceholder}>{form?.cityId ? cityName(form.cityId) : '请选择'}</Text>
                </Pressable>
              </FormField>
              <FormField label="所属站点" required style={styles.formHalf}>
                <Pressable style={[styles.formSelect, !form?.cityId && styles.selectDisabled]} onPress={() => form?.cityId && setPicker('formStation')}>
                  <Text style={form?.stationId ? styles.selectText : styles.selectPlaceholder}>{form?.stationId ? stationName(form.stationId, formStations, '请选择') : '请选择'}</Text>
                </Pressable>
              </FormField>
            </View>
            <FormField label="产品名称" required>
              <TextInput style={styles.input} placeholder="请输入产品名称" placeholderTextColor="#999" value={form?.product_name} onChangeText={value => setForm(current => current ? { ...current, product_name: value } : current)} />
            </FormField>
            <View style={styles.formPair}>
              <FormField label="微信号" required style={styles.formHalf}>
                <TextInput style={styles.input} placeholder="请输入微信号" placeholderTextColor="#999" value={form?.wechat} onChangeText={value => setForm(current => current ? { ...current, wechat: value } : current)} />
              </FormField>
              <FormField label="电话" required style={styles.formHalf}>
                <TextInput style={styles.input} placeholder="请输入电话" placeholderTextColor="#999" keyboardType="phone-pad" value={form?.phone} onChangeText={value => setForm(current => current ? { ...current, phone: value } : current)} />
              </FormField>
            </View>
            <FormField label="地址" required>
              <TextInput style={styles.input} placeholder="请输入地址" placeholderTextColor="#999" value={form?.address} onChangeText={value => setForm(current => current ? { ...current, address: value } : current)} />
            </FormField>
            <View style={styles.formPair}>
              <FormField label="货架总量" style={styles.formHalf}>
                <TextInput style={styles.input} keyboardType="number-pad" value={form?.total_quantity} onChangeText={value => setForm(current => current ? { ...current, total_quantity: value } : current)} />
              </FormField>
              <FormField label="预警数量" style={styles.formHalf}>
                <TextInput style={styles.input} keyboardType="number-pad" value={form?.warning_quantity} onChangeText={value => setForm(current => current ? { ...current, warning_quantity: value } : current)} />
              </FormField>
              <FormField label="单次订购" style={styles.formHalf}>
                <TextInput style={styles.input} keyboardType="number-pad" value={form?.order_quantity} onChangeText={value => setForm(current => current ? { ...current, order_quantity: value } : current)} />
              </FormField>
            </View>
            {form?.device_type === 'tea_bar' ? (
              <FormField label="当前库存" required>
                <TextInput style={styles.input} keyboardType="number-pad" value={form.current_quantity} onChangeText={value => setForm(current => current ? { ...current, current_quantity: value } : current)} />
                <Text style={styles.formHint}>茶吧机库存可手动填写，MQTT 按 1→0 自动扣减。</Text>
              </FormField>
            ) : null}
            <View style={styles.formPair}>
              <FormField label="流量卡号" required style={styles.formHalf}>
                <TextInput style={styles.input} placeholder="请输入流量卡号" placeholderTextColor="#999" value={form?.sim_card_number} onChangeText={value => setForm(current => current ? { ...current, sim_card_number: value } : current)} />
              </FormField>
              <FormField label="流量卡到期日" required style={styles.formHalf}>
                <Pressable style={styles.formSelect} onPress={() => setExpiryPicker(true)}>
                  <Text style={form?.sim_card_expiry ? styles.selectText : styles.selectPlaceholder}>{form?.sim_card_expiry || '请选择'}</Text>
                </Pressable>
              </FormField>
            </View>
            <FormField label="支付状态">
              <View style={styles.chipRow}>
                {['', '未支付', '已联系'].map(value => (
                  <Chip key={`form-pay-${value || 'empty'}`} label={value || '不填'} active={form?.payment_method === value} onPress={() => setForm(current => current ? { ...current, payment_method: value } : current)} />
                ))}
              </View>
            </FormField>
            <FormField label="备注">
              <TextInput style={[styles.input, styles.remarkInput]} placeholder="请输入备注" placeholderTextColor="#999" multiline value={form?.remark} onChangeText={value => setForm(current => current ? { ...current, remark: value } : current)} />
            </FormField>
            <View style={styles.formPair}>
              <FormField label="电压" style={styles.formHalf}>
                <TextInput style={styles.input} keyboardType="decimal-pad" value={form?.voltage} onChangeText={value => setForm(current => current ? { ...current, voltage: value } : current)} />
              </FormField>
              <FormField label="信号" style={styles.formHalf}>
                <TextInput style={styles.input} keyboardType="number-pad" value={form?.signal_strength} onChangeText={value => setForm(current => current ? { ...current, signal_strength: value } : current)} />
              </FormField>
            </View>
            <FormField label="版本">
              <TextInput style={styles.input} placeholder="请输入版本" placeholderTextColor="#999" value={form?.version} onChangeText={value => setForm(current => current ? { ...current, version: value } : current)} />
            </FormField>
          </ScrollView>
        </View>
      </Modal>
      {expiryPicker && form ? (
        <DateTimePicker
          value={form.sim_card_expiry ? new Date(`${form.sim_card_expiry}T00:00:00`) : new Date()}
          mode="date"
          display="default"
          positiveButton={{ label: '确定', textColor: ACCENT }}
          negativeButton={{ label: '取消', textColor: '#666' }}
          onChange={onExpiryPicked}
        />
      ) : null}

      <Modal visible={logLines !== null} transparent animationType="fade" onRequestClose={() => setLogLines(null)}>
        <Pressable style={styles.backdrop} onPress={() => setLogLines(null)}>
          <Pressable style={styles.sheet} onPress={event => event.stopPropagation()}>
            <Text style={styles.formTitle}>{logTitle}</Text>
            <ScrollView>
              {(logLines || []).map(line => <Text key={line} style={styles.logLine}>{line}</Text>)}
            </ScrollView>
          </Pressable>
        </Pressable>
      </Modal>

      <Modal visible={!!teaBarShelf} transparent animationType="fade" onRequestClose={() => setTeaBarShelf(null)}>
        <Pressable style={styles.backdrop} onPress={() => setTeaBarShelf(null)}>
          <Pressable style={styles.sheet} onPress={event => event.stopPropagation()}>
            <Text style={styles.formTitle}>设置当前库存</Text>
            <Text style={styles.detailLine}>{teaBarShelf?.iccid}</Text>
            <TextInput style={styles.input} keyboardType="number-pad" value={teaBarQty} onChangeText={setTeaBarQty} />
            <Pressable style={styles.primaryButton} onPress={saveTeaBarQty}>
              <Text style={styles.primaryButtonText}>保存</Text>
            </Pressable>
          </Pressable>
        </Pressable>
      </Modal>
    </View>
  );
}

const styles = StyleSheet.create({
  pane: { flex: 1, backgroundColor: '#fff' },
  searchPanel: {
    backgroundColor: '#fff',
    paddingHorizontal: 12,
    paddingTop: 8,
    paddingBottom: 6,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#e5e5e5',
    gap: 8,
  },
  searchRow: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  searchInput: {
    flex: 1,
    borderWidth: 1,
    borderColor: '#ddd',
    borderRadius: 6,
    paddingHorizontal: 10,
    paddingVertical: 8,
    fontSize: 14,
    color: '#222',
    backgroundColor: '#fafafa',
  },
  searchButton: { borderWidth: 1, borderColor: ACCENT, borderRadius: 6, paddingHorizontal: 10, paddingVertical: 8 },
  searchButtonText: { color: ACCENT, fontWeight: '700', fontSize: 13 },
  filterButton: { borderWidth: 1, borderColor: '#ddd', borderRadius: 6, paddingHorizontal: 8, paddingVertical: 8 },
  filterButtonActive: { borderColor: ACCENT, backgroundColor: '#eff6ff' },
  filterButtonText: { color: '#666', fontSize: 13, fontWeight: '600' },
  filterButtonTextActive: { color: ACCENT },
  clearText: { color: '#888', fontSize: 13 },
  filterBox: { gap: 6 },
  fieldLabel: { color: '#888', fontSize: 12, marginTop: 2 },
  fieldRow: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  chipRow: { flexDirection: 'row', flexWrap: 'wrap', gap: 6 },
  chip: { borderWidth: 1, borderColor: '#ddd', borderRadius: 4, paddingHorizontal: 8, paddingVertical: 4, backgroundColor: '#fafafa' },
  chipActive: { borderColor: ACCENT, backgroundColor: '#eff6ff' },
  chipText: { color: '#666', fontSize: 12 },
  chipTextActive: { color: ACCENT, fontWeight: '700' },
  select: { flex: 1, borderWidth: 1, borderColor: '#ddd', borderRadius: 6, paddingHorizontal: 10, paddingVertical: 8, backgroundColor: '#fafafa' },
  selectDisabled: { opacity: 0.45 },
  selectText: { color: '#222', fontSize: 13 },
  rangeInput: { flex: 1, borderWidth: 1, borderColor: '#ddd', borderRadius: 6, paddingHorizontal: 10, paddingVertical: 8, fontSize: 13, color: '#222' },
  rangeDash: { color: '#999' },
  filterActions: { flexDirection: 'row', justifyContent: 'flex-end', gap: 8, marginTop: 4 },
  ghostButton: { paddingHorizontal: 12, paddingVertical: 8 },
  ghostButtonText: { color: '#666', fontWeight: '600' },
  primaryButton: { backgroundColor: ACCENT, borderRadius: 6, paddingHorizontal: 16, paddingVertical: 8 },
  primaryButtonText: { color: '#fff', fontWeight: '700' },
  hintRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  hint: { color: '#999', fontSize: 12 },
  addText: { color: ACCENT, fontWeight: '700', fontSize: 13 },
  list: { flexGrow: 1 },
  card: { paddingHorizontal: 16, paddingVertical: 12, borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: '#e8e8e8', gap: 6, backgroundColor: '#fff' },
  cardAlt: { backgroundColor: '#f3f4f6' },
  titleRow: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  title: { flex: 1, color: '#222', fontSize: 16, fontWeight: '700' },
  tag: { color: '#475569', backgroundColor: '#f1f5f9', borderRadius: 4, overflow: 'hidden', paddingHorizontal: 6, paddingVertical: 2, fontSize: 11 },
  tagTea: { color: '#92400e', backgroundColor: '#fef3c7' },
  address: { color: '#333', fontSize: 14 },
  summaryRow: { flexDirection: 'row', alignItems: 'center', gap: 8, flexWrap: 'wrap' },
  summary: { color: '#444', fontSize: 13 },
  danger: { color: DANGER, fontWeight: '700' },
  miniButton: { borderWidth: 1, borderColor: ACCENT, borderRadius: 4, paddingHorizontal: 8, paddingVertical: 3 },
  miniButtonText: { color: ACCENT, fontSize: 12, fontWeight: '700' },
  detail: { gap: 4, paddingTop: 4 },
  detailLine: { color: '#444', fontSize: 13, lineHeight: 20 },
  actionRow: { flexDirection: 'row', flexWrap: 'wrap', gap: 14, marginTop: 4 },
  actionText: { color: ACCENT, fontWeight: '700', fontSize: 13 },
  dangerAction: { color: DANGER, fontWeight: '700', fontSize: 13 },
  empty: { textAlign: 'center', color: '#999', marginTop: 40 },
  footer: { marginVertical: 16 },
  backdrop: { flex: 1, backgroundColor: 'rgba(0,0,0,0.35)', justifyContent: 'flex-end' },
  sheet: { maxHeight: '70%', backgroundColor: '#fff', borderTopLeftRadius: 12, borderTopRightRadius: 12, padding: 16 },
  option: { paddingVertical: 12, borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: '#eee' },
  optionText: { fontSize: 15, color: '#222' },
  formPage: { flex: 1, backgroundColor: '#fff', paddingTop: Platform.OS === 'android' ? 28 : 48 },
  formHeader: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', paddingHorizontal: 16, paddingBottom: 8 },
  formTitle: { fontSize: 16, fontWeight: '700', color: '#222' },
  formBody: { paddingHorizontal: 16, paddingTop: 8, paddingBottom: 48, gap: 14 },
  formField: { gap: 6 },
  formLabel: { color: '#334155', fontSize: 14, fontWeight: '600' },
  requiredMark: { color: '#e54d42' },
  formPair: { flexDirection: 'row', alignItems: 'flex-start', gap: 10 },
  formHalf: { flex: 1 },
  formSelect: { borderWidth: 1, borderColor: '#ddd', borderRadius: 6, paddingHorizontal: 10, paddingVertical: 10, backgroundColor: '#fafafa' },
  selectPlaceholder: { color: '#999', fontSize: 14 },
  formHint: { color: '#888', fontSize: 12, lineHeight: 18 },
  input: { borderWidth: 1, borderColor: '#ddd', borderRadius: 6, paddingHorizontal: 10, paddingVertical: 9, fontSize: 14, color: '#222', backgroundColor: '#fff' },
  remarkInput: { minHeight: 72, textAlignVertical: 'top' },
  logLine: { color: '#333', fontSize: 14, paddingVertical: 8, borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: '#eee' },
});
