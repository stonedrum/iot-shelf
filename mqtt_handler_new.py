import json
import hashlib
import time
import logging
import smtplib
import urllib.request
from uuid import uuid4
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float, Numeric, ForeignKey, BigInteger, Boolean, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from typing import Optional, List
import configparser
import os
from dotenv import load_dotenv

# 配置日志
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("mqtt_handler.log"),  # 输出到文件
        logging.StreamHandler()  # 同时输出到控制台
    ]
)
logger = logging.getLogger(__name__)
DEVICE_TYPE_SHELF = "shelf"
DEVICE_TYPE_TEA_BAR = "tea_bar"

# 读取配置文件
config = configparser.ConfigParser()
config.read('confignew.ini')

# 数据库配置
DB_HOST = config.get('database', 'host')
DB_PORT = config.get('database', 'port')
DB_USER = config.get('database', 'user')
DB_PASSWORD = config.get('database', 'password')
DB_NAME = config.get('database', 'name')

# MQTT配置
MQTT_BROKER = config.get('mqtt', 'broker')
MQTT_PORT = config.getint('mqtt', 'port')
MQTT_USER = config.get('mqtt', 'user')
MQTT_PASSWORD = config.get('mqtt', 'password')
MQTT_TOPIC = config.get('mqtt', 'topic')
MQTT_CLIENT_ID = config.get('mqtt', 'client_id', fallback='water_shelf_client118')

# 邮件配置
SMTP_HOST = config.get('email', 'smtp_host', fallback='smtp.163.com')
SMTP_PORT = config.getint('email', 'smtp_port', fallback=465)
SMTP_USER = config.get('email', 'smtp_user', fallback='13735447734@163.com')
SMTP_PASSWORD = config.get('email', 'smtp_password', fallback='')
SMTP_SENDER_NAME = config.get('email', 'sender_name', fallback='搬夫科技')
LOW_VOLTAGE_THRESHOLD = config.getfloat('email', 'low_voltage_threshold', fallback=36.1)

# 个推 RestAPI V2 配置（Master Secret 仅保存在服务端）
GETUI_APP_ID = config.get('getui', 'app_id', fallback='').strip()
GETUI_APP_KEY = config.get('getui', 'app_key', fallback='').strip()
GETUI_MASTER_SECRET = config.get('getui', 'master_secret', fallback='').strip()
GETUI_BASE_URL = config.get(
    'getui', 'base_url', fallback='https://restapi.getui.com'
).rstrip('/')
_getui_auth_token = None
_getui_auth_token_valid_until = 0.0

# 数据库连接
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 数据库模型 - 与backend项目保持一致
class Station(Base):
    __tablename__ = "stations"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    station_name = Column(String(100), nullable=False)
    city_id = Column(Integer, ForeignKey("cities.id"), nullable=False)
    remark = Column(String(255))
    contact_name = Column(String(50), nullable=False)
    contact_phone = Column(String(20), nullable=False)
    email = Column(String(100), default="", nullable=False, comment="预警通知邮箱")
    created_by = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_by = Column(String(50), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    is_deleted = Column(Integer, default=0, nullable=False)

class Shelf(Base):
    __tablename__ = "shelves"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    iccid = Column(String(50), unique=True, nullable=False, index=True, comment="设备号")
    wechat = Column(String(50), nullable=False)
    phone = Column(String(20), nullable=False)
    address = Column(String(255), nullable=False)
    total_quantity = Column(Integer, nullable=False, comment="货架总量")
    product_name = Column(String(100), nullable=False)
    order_quantity = Column(Integer, nullable=False, comment="单次订购数量")
    current_quantity = Column(Integer, default=0, nullable=False, comment="现有数量")
    warning_quantity = Column(Integer, default=3, nullable=False, comment="预警数量")
    delivery_status = Column(Integer, default=0, nullable=False, comment="发货状态(0:默认,1:已发货)")
    voltage = Column(Numeric(5, 2), default=0, nullable=False)
    online_status = Column(Integer, default=0, nullable=False, comment="在线状态(0:离线,1:在线)")
    push_time = Column(DateTime)
    sim_card_number = Column(String(50), nullable=False, comment="流量卡号")
    sim_card_expiry = Column(DateTime, nullable=False, comment="流量卡到期日")
    signal_strength = Column(Integer, default=0, nullable=False)
    longitude = Column(Numeric(10, 6))
    latitude = Column(Numeric(10, 6))
    version = Column(String(20))
    device_type = Column(String(20), default=DEVICE_TYPE_SHELF, nullable=False, comment="设备类型(shelf:货架,tea_bar:茶吧机)")
    last_switch_bitmap = Column(String(8), nullable=True, comment="茶吧机上次8路开关状态位图")
    remark = Column(String(255), default="")
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False, index=True)
    created_by = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_by = Column(String(50), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    is_deleted = Column(Integer, default=0, nullable=False)

class ShelfLog(Base):
    __tablename__ = "shelf_logs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    shelf_id = Column(Integer, ForeignKey("shelves.id"), nullable=False)
    shelf_name = Column(String(100), nullable=False, comment="货架名称(ICCID)")
    log_time = Column(DateTime, default=func.now(), nullable=False)
    current_quantity = Column(Integer, nullable=False)
    created_by = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_by = Column(String(50), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    is_deleted = Column(Integer, default=0, nullable=False)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    role = Column(String(20), nullable=False)
    is_deleted = Column(Integer, default=0, nullable=False)


class UserCity(Base):
    __tablename__ = "user_cities"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    city_id = Column(Integer, nullable=False)
    is_deleted = Column(Integer, default=0, nullable=False)


class PushToken(Base):
    __tablename__ = "push_tokens"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    token = Column(String(255), unique=True, nullable=False)
    enabled = Column(Integer, default=1, nullable=False)


class WaterDeliveryOrder(Base):
    __tablename__ = "water_delivery_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_no = Column(String(40), unique=True, nullable=False)
    shelf_id = Column(Integer, ForeignKey("shelves.id"), nullable=False)
    station_id = Column(Integer, ForeignKey("stations.id"), nullable=False)
    source = Column(String(20), nullable=False, default="auto")
    status = Column(String(20), nullable=False, default="pending")
    payment_status = Column(String(20), nullable=False, default="unpaid")
    requested_quantity = Column(Integer, nullable=False)
    delivered_quantity = Column(Integer)
    trigger_quantity = Column(Integer, nullable=False)
    stock_before_delivery = Column(Integer)
    stock_after_delivery = Column(Integer)
    active_key = Column(String(64), unique=True, nullable=True)
    remark = Column(String(255), default="")
    delivered_at = Column(DateTime)
    delivered_by = Column(String(50))
    created_by = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_by = Column(String(50), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    is_deleted = Column(Integer, default=0, nullable=False)

# 数据库操作函数
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def parse_switch_statuses(m_field: List[str]) -> List[int]:
    statuses: List[int] = []
    for index in range(4, 12):
        raw_value = m_field[index] if index < len(m_field) else ""
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            parsed = 0
        statuses.append(1 if parsed == 1 else 0)
    return statuses


def send_low_stock_alert_email(station, shelf):
    """货架库存低于预警值时，向站点邮箱发送补货通知"""
    to_email = (station.email or "").strip()
    if not to_email:
        logger.warning(f"站点 {station.station_name}(ID:{station.id}) 未配置邮箱，跳过预警邮件")
        return False
    if not SMTP_PASSWORD:
        logger.warning("未配置邮件 SMTP 密码，跳过预警邮件发送")
        return False

    voltage = float(shelf.voltage or 0)
    if voltage < LOW_VOLTAGE_THRESHOLD:
        voltage_html = (
            f'<strong style="color:#d32f2f;">{voltage:.2f} V</strong>'
            f' <span style="color:#d32f2f;">（低于阈值 {LOW_VOLTAGE_THRESHOLD}V）</span>'
        )
    else:
        voltage_html = f"{voltage:.2f} V"

    subject = f"【{SMTP_SENDER_NAME}】货架库存预警 - {shelf.iccid}"
    body = f"""
    <div style="font-family:Arial,sans-serif;line-height:1.6;color:#333;">
      <p>尊敬的客户，您好！</p>
      <p>站点「{station.station_name}」下的货架设备当前余量不足，请及时安排补货。</p>
      <p>
        设备号（ICCID）：{shelf.iccid}<br>
        微信号：{shelf.wechat}<br>
        电话：{shelf.phone}<br>
        产品名称：{shelf.product_name}<br>
        安装地址：{shelf.address}<br>
        当前余量：{shelf.current_quantity}<br>
        预警阈值：{shelf.warning_quantity}<br>
        建议补货数量：{max(int(shelf.total_quantity or 0) - int(shelf.current_quantity or 0), 1)}<br>
        当前电压：{voltage_html}
      </p>
      <p>当前库存已低于预警线，请尽快补货，避免缺货影响正常运营。</p>
      <p>—— {SMTP_SENDER_NAME}<br>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    """

    try:
        msg = MIMEText(body, "html", "utf-8")
        msg["From"] = formataddr((str(Header(SMTP_SENDER_NAME, "utf-8")), SMTP_USER))
        msg["To"] = to_email
        msg["Subject"] = Header(subject, "utf-8")

        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, [to_email], msg.as_string())

        logger.info(f"已向 {to_email} 发送货架 {shelf.iccid} 库存预警邮件")
        return True
    except Exception as e:
        logger.error(f"发送库存预警邮件失败: {str(e)}")
        return False


def create_auto_water_order(db, shelf):
    """低库存时创建订单；每个货架最多保留一张待配送订单。"""
    active_key = f"shelf:{shelf.id}"
    existing = db.query(WaterDeliveryOrder).filter(
        WaterDeliveryOrder.active_key == active_key,
        WaterDeliveryOrder.is_deleted == 0
    ).first()
    if existing:
        return None

    order = WaterDeliveryOrder(
        order_no=f"WS{datetime.now().strftime('%Y%m%d%H%M%S')}{uuid4().hex[:6].upper()}",
        shelf_id=shelf.id,
        station_id=shelf.station_id,
        source="auto",
        status="pending",
        payment_status="unpaid",
        requested_quantity=max(int(shelf.total_quantity or 0) - int(shelf.current_quantity or 0), 1),
        trigger_quantity=shelf.current_quantity,
        active_key=active_key,
        remark="库存达到预警值，MQTT自动生成",
        created_by="mqtt_handler",
        updated_by="mqtt_handler"
    )
    try:
        # 使用保存点处理多个MQTT实例同时建单造成的唯一键冲突，
        # 冲突不会回滚本次库存与货架日志更新。
        with db.begin_nested():
            db.add(order)
            db.flush()
        return order
    except IntegrityError:
        logger.info(f"货架 {shelf.iccid} 已由其他进程生成待配送订单")
        return None


def get_getui_auth_token():
    """获取并缓存个推 RestAPI V2 鉴权 token。"""
    global _getui_auth_token, _getui_auth_token_valid_until
    if not all((GETUI_APP_ID, GETUI_APP_KEY, GETUI_MASTER_SECRET)):
        logger.warning("未配置个推服务端参数，跳过 App 推送")
        return None
    if _getui_auth_token and time.monotonic() < _getui_auth_token_valid_until:
        return _getui_auth_token

    timestamp = str(int(time.time() * 1000))
    sign = hashlib.sha256(
        f"{GETUI_APP_KEY}{timestamp}{GETUI_MASTER_SECRET}".encode("utf-8")
    ).hexdigest()
    try:
        request = urllib.request.Request(
            f"{GETUI_BASE_URL}/v2/{GETUI_APP_ID}/auth",
            data=json.dumps({
                "sign": sign,
                "timestamp": timestamp,
                "appkey": GETUI_APP_KEY
            }).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("code") != 0 or not result.get("data", {}).get("token"):
            logger.error(f"个推鉴权失败: {result}")
            return None
        _getui_auth_token = result["data"]["token"]
        _getui_auth_token_valid_until = time.monotonic() + 23 * 60 * 60
        return _getui_auth_token
    except Exception as error:
        logger.error(f"个推鉴权请求失败: {error}")
        return None


def send_getui_to_cid(cid, title, body, payload):
    token = get_getui_auth_token()
    if not token:
        return False

    payload_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    message = {
        "request_id": uuid4().hex,
        "settings": {"ttl": 24 * 60 * 60 * 1000},
        "audience": {"cid": [cid]},
        "push_message": {
            "notification": {
                "title": title,
                "body": body,
                "click_type": "payload",
                "payload": payload_text,
                "channel_id": "water_orders",
                "channel_name": "送水订单",
                "channel_level": 4
            }
        },
        "push_channel": {
            "android": {
                "ups": {
                    "notification": {
                        "title": title,
                        "body": body,
                        "click_type": "payload",
                        "payload": payload_text
                    }
                }
            },
            "ios": {
                "type": "notify",
                "aps": {
                    "alert": {"title": title, "body": body},
                    "content-available": 0,
                    "sound": "default"
                },
                "auto_badge": "+1",
                "payload": payload_text
            }
        }
    }
    try:
        request = urllib.request.Request(
            f"{GETUI_BASE_URL}/v2/{GETUI_APP_ID}/push/single/cid",
            data=json.dumps(message, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json", "token": token},
            method="POST"
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("code") != 0:
            logger.error(f"个推 CID {cid} 推送失败: {result}")
            return False
        return True
    except Exception as error:
        logger.error(f"个推 CID {cid} 推送请求失败: {error}")
        return False


def send_new_order_push(db, station, shelf, order):
    """通过个推提醒有权限的 App 用户。"""
    allowed_user_ids = {
        user.id for user in db.query(User).filter(
            User.role == "admin",
            User.is_deleted == 0
        ).all()
    }
    allowed_user_ids.update(
        row.user_id for row in db.query(UserCity).filter(
            UserCity.city_id == station.city_id,
            UserCity.is_deleted == 0
        ).all()
    )
    if not allowed_user_ids:
        return

    tokens = db.query(PushToken).filter(
        PushToken.user_id.in_(allowed_user_ids),
        PushToken.enabled == 1
    ).all()
    if not tokens:
        return

    title = "新的送水订单"
    body = f"{station.station_name} · {shelf.iccid} 当前余量 {shelf.current_quantity}"
    payload = {
        "type": "water_order",
        "orderId": order.id,
        "orderNo": order.order_no
    }
    success_count = sum(
        send_getui_to_cid(token.token, title, body, payload)
        for token in tokens
        if token.token and token.token.strip()
    )
    logger.info(
        f"新订单 {order.order_no} 已通过个推发送到 {success_count} 台设备"
    )


def parse_and_save_status(db, message_data):
    """解析MQTT消息并更新货架状态"""
    try:
        # 提取货架ICCID
        iccid = message_data.get('m', '').split('&')[1] if 'm' in message_data else None
        iccid = message_data.get('f', '') 
        if not iccid:
            logger.warning("消息中未包含ICCID")
            return None
            
        # 查询对应的货架
        shelf = db.query(Shelf).filter(Shelf.iccid == iccid, Shelf.is_deleted == 0).first()
        if not shelf:
            logger.warning(f"未找到ICCID为 {iccid} 的货架")
            return None
            
        # 解析m字段
        m_field = message_data.get('m', '').split('&')
        
        # 构建状态数据
        signal_strength = int(m_field[0]) if len(m_field) > 0 and m_field[0] else None
        # 流量卡号: m字段第1个"&"后到第2个"&"前
        sim_card_number = m_field[1].strip() if len(m_field) > 1 and m_field[1] else None
        longitude = float(m_field[2].split(',')[0]) if len(m_field) > 2 and ',' in m_field[2] else None
        latitude = float(m_field[2].split(',')[1]) if len(m_field) > 2 and ',' in m_field[2] else None
        voltage = float(m_field[3]) if len(m_field) > 3 and m_field[3] else None
        switch_statuses = parse_switch_statuses(m_field)
        version = m_field[-1] if m_field else None

        prev_quantity = shelf.current_quantity
        warning_quantity = shelf.warning_quantity

        # 计算当前开关快照值（用于老设备）
        snapshot_quantity = sum(switch_statuses)
        switch_bitmap = "".join(str(value) for value in switch_statuses)
        device_type = (shelf.device_type or DEVICE_TYPE_SHELF).strip().lower()

        # 老设备: MQTT快照值直接覆盖当前库存
        if device_type == DEVICE_TYPE_SHELF:
            if snapshot_quantity > shelf.current_quantity:
                shelf.delivery_status = 0
            shelf.current_quantity = snapshot_quantity
            shelf.last_switch_bitmap = None
        # 新设备(茶吧机): 仅在1->0时扣减库存
        elif device_type == DEVICE_TYPE_TEA_BAR:
            prev_bitmap = shelf.last_switch_bitmap or ""
            if len(prev_bitmap) == 8:
                down_edges = sum(
                    1 for prev, curr in zip(prev_bitmap, switch_bitmap)
                    if prev == "1" and curr == "0"
                )
                if down_edges > 0:
                    shelf.current_quantity = shelf.current_quantity - down_edges
            # 首帧仅初始化状态，不扣减
            shelf.last_switch_bitmap = switch_bitmap
        else:
            logger.warning(f"货架 {iccid} 存在未知设备类型 {device_type}，按货架逻辑处理")
            shelf.current_quantity = snapshot_quantity
            shelf.last_switch_bitmap = None

        # 兜底: 库存为空时设为0；货架设备仍限制不允许负数
        if shelf.current_quantity is None:
            shelf.current_quantity = 0
        elif device_type != DEVICE_TYPE_TEA_BAR and shelf.current_quantity < 0:
            shelf.current_quantity = 0

        # 更新货架状态
        shelf.online_status = 1  # 设置为在线
        shelf.voltage = voltage if voltage is not None else shelf.voltage
        shelf.signal_strength = signal_strength if signal_strength is not None else shelf.signal_strength
        shelf.sim_card_number = sim_card_number if sim_card_number is not None else shelf.sim_card_number
        shelf.version = version if version is not None else shelf.version
        shelf.updated_at = datetime.now()
        shelf.push_time = datetime.now()
  
        
        # 更新货架的经纬度（如果消息中包含）
        if longitude and latitude:
            shelf.longitude = longitude
            shelf.latitude = latitude
        
        # 创建货架日志
        shelf_log = ShelfLog(
            shelf_id=shelf.id,
            shelf_name=shelf.iccid,
            log_time=datetime.now(),
            current_quantity=shelf.current_quantity,
            created_by="mqtt_handler",
            updated_by="mqtt_handler"
        )

        is_low_stock = shelf.current_quantity <= warning_quantity
        quantity_decreased = shelf.current_quantity < prev_quantity
        new_order = None
        if is_low_stock and quantity_decreased:
            new_order = create_auto_water_order(db, shelf)

        db.add(shelf_log)
        db.commit()
        db.refresh(shelf)
        db.refresh(shelf_log)

        # 库存预警邮件:
        # 1) 首次跌破预警线时通知
        # 2) 已处于预警线及以下时，余量每再减少一次也再次通知
        if is_low_stock and quantity_decreased:
            station = db.query(Station).filter(
                Station.id == shelf.station_id,
                Station.is_deleted == 0
            ).first()
            if station:
                send_low_stock_alert_email(station, shelf)
                if new_order:
                    db.refresh(new_order)
                    send_new_order_push(db, station, shelf, new_order)
        
        logger.info(f"成功更新货架 {iccid} 的状态，设备类型: {device_type}, 当前数量: {shelf.current_quantity}")
        return shelf
    except Exception as e:
        db.rollback()
        logger.error(f"解析并保存状态失败: {str(e)}")
        return None

# MQTT消息处理回调函数
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("已成功连接到MQTT Broker")
        client.subscribe(MQTT_TOPIC)
        logger.info(f"已订阅主题: {MQTT_TOPIC}")
    else:
        logger.error(f"连接失败，错误代码: {rc}")

def on_message(client, userdata, msg):
    logger.info(f"收到消息 - 主题: {msg.topic}, 时间: {datetime.now()}")
    
    # 获取数据库会话
    db = next(get_db())
    
    try:
        # 解析消息
        payload = msg.payload.decode('utf-8')
        message_data = json.loads(payload)
        
        # 解析并保存状态数据
        parse_and_save_status(db, message_data)
    except json.JSONDecodeError as e:
        logger.error(f"解析消息失败: {str(e)}")
    except Exception as e:
        logger.error(f"处理消息时发生错误: {str(e)}")
    finally:
        # 关闭数据库会话
        db.close()

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning("意外断开连接")
    logger.info("已断开与MQTT Broker的连接")

def main():
    # 确保数据库表存在
    try:
        Base.metadata.create_all(engine)
        logger.info("数据库表检查完成，所有表已存在或已创建。")
    except Exception as e:
        logger.error(f"创建数据库表时发生错误: {str(e)}")
        return  # 如果表创建失败，程序无法继续，直接退出
    
    # 创建MQTT客户端
    client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    
    # 设置认证信息
    if MQTT_USER and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    
    # 设置回调函数
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    # 连接到MQTT Broker
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        logger.error(f"连接到MQTT Broker失败: {str(e)}")
        return
    
    # 保持连接并处理消息
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("用户中断程序")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()
