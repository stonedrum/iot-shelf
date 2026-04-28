import json
import time
import logging
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float, Numeric, ForeignKey, BigInteger, Boolean, func
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
MQTT_CLIENT_ID = config.get('mqtt', 'client_id', fallback='water_shelf_client11')

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
        
        db.add(shelf_log)
        db.commit()
        db.refresh(shelf)
        db.refresh(shelf_log)
        
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
