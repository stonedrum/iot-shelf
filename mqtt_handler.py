import json
import time
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float, ForeignKey, BigInteger, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from typing import Optional
import configparser

# 读取配置文件
config = configparser.ConfigParser()
config.read('config.ini')

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
MQTT_CLIENT_ID = config.get('mqtt', 'client_id', fallback='water_shelf_client')

# 数据库连接
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 数据库模型 - 与之前定义的表结构对应
class Customer(Base):
    __tablename__ = "customer"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    contact_person = Column(String(50), nullable=True)
    phone = Column(String(20), nullable=True)
    address = Column(Text, nullable=True)
    payment_method = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    remark = Column(Text, nullable=True)
    
    shelves = relationship("Shelf", back_populates="customer")

class Shelf(Base):
    __tablename__ = "shelf"
    
    id = Column(Integer, primary_key=True, index=True)
    shelf_code = Column(String(50), unique=True, nullable=False, index=True)
    iccid = Column(String(50), unique=True, nullable=True, index=True)
    name = Column(String(100), nullable=True)
    customer_id = Column(Integer, ForeignKey("customer.id", ondelete="SET NULL"), nullable=True)
    location = Column(Text, nullable=True)
    longitude = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    status = Column(Integer, default=0)
    voltage = Column(Float, nullable=True)
    water_num = Column(Integer, default=0)
    
    customer = relationship("Customer", back_populates="shelves")
    status_records = relationship("ShelfStatus", back_populates="shelf", cascade="all, delete-orphan")

class ShelfStatus(Base):
    __tablename__ = "shelf_status"
    
    id = Column(BigInteger, primary_key=True, index=True)
    shelf_id = Column(Integer, ForeignKey("shelf.id", ondelete="CASCADE"), nullable=False)
    signal_strength = Column(Integer, nullable=True)
    iccid = Column(String(50), nullable=True)
    longitude = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    voltage = Column(Float, nullable=True)
    switch1_status = Column(Integer, default=0)
    switch2_status = Column(Integer, default=0)
    switch3_status = Column(Integer, default=0)
    switch4_status = Column(Integer, default=0)
    switch5_status = Column(Integer, default=0)
    switch6_status = Column(Integer, default=0)
    switch7_status = Column(Integer, default=0)
    switch8_status = Column(Integer, default=0)
    version = Column(String(50), nullable=True)
    collected_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    
    shelf = relationship("Shelf", back_populates="status_records")

class MqttRawMessage(Base):
    __tablename__ = "mqtt_raw_message"
    
    id = Column(BigInteger, primary_key=True, index=True)
    topic = Column(String(255), nullable=False)
    payload = Column(Text, nullable=False)
    received_at = Column(DateTime, default=datetime.now())
    shelf_code = Column(String(50), nullable=True, index=True)

# 数据库操作函数
def get_db():
    db = SessionLocal()
    

    try:
        yield db
    finally:
        db.close()

def save_raw_message(db, topic, payload, shelf_code=None):
    """保存原始MQTT消息到数据库"""
    try:
        raw_msg = MqttRawMessage(
            topic=topic,
            payload=payload,
            shelf_code=shelf_code
        )
        db.add(raw_msg)
        db.commit()
        db.refresh(raw_msg)
        return raw_msg
    except Exception as e:
        db.rollback()
        print(f"保存原始消息失败: {str(e)}")
        return None

def parse_and_save_status(db, message_data):
    """解析消息并保存到货架状态表"""
    try:
        # 提取货架编号
        shelf_code = message_data.get('f')
        if not shelf_code:
            print("消息中未包含货架编号(t字段)")
            return None
            
        # 查询对应的货架
        shelf = db.query(Shelf).filter(Shelf.shelf_code == shelf_code).first()
        if not shelf:
            print(f"未找到编号为 {shelf_code} 的货架")
            return None
            
        # 解析m字段
        m_field = message_data.get('m', '').split('&')
        
        # 构建状态记录
        status_record = ShelfStatus(
            shelf_id=shelf.id,
            signal_strength=int(m_field[0]) if len(m_field) > 0 and m_field[0] else None,
            iccid=m_field[1] if len(m_field) > 1 else None,
            longitude=float(m_field[2].split(',')[0]) if len(m_field) > 2 and ',' in m_field[2] else None,
            latitude=float(m_field[2].split(',')[1]) if len(m_field) > 2 and ',' in m_field[2] else None,
            voltage=float(m_field[3]) if len(m_field) > 3 and m_field[3] else None,
            switch1_status=int(m_field[4]) if len(m_field) > 4 and m_field[4] else 0,
            switch2_status=int(m_field[5]) if len(m_field) > 5 and m_field[5] else 0,
            switch3_status=int(m_field[6]) if len(m_field) > 6 and m_field[6] else 0,
            switch4_status=int(m_field[7]) if len(m_field) > 7 and m_field[7] else 0,
            switch5_status=int(m_field[8]) if len(m_field) > 8 and m_field[8] else 0,
            switch6_status=int(m_field[9]) if len(m_field) > 9 and m_field[9] else 0,
            switch7_status=int(m_field[10]) if len(m_field) > 10 and m_field[10] else 0,
            switch8_status=int(m_field[11]) if len(m_field) > 11 and m_field[11] else 0,
            version=m_field[-1] if m_field else None,
            collected_at=datetime.now()  # 使用当前时间作为采集时间
        )

        # 得到8个状态开关相加的值
        switch_status_sum = sum([int(m_field[i]) for i in range(4,12) if m_field[i]])

        # 更新货架的状态
        shelf.iccid = status_record.iccid
        shelf.status = 1
        shelf.water_num = switch_status_sum
        shelf.voltage = status_record.voltage
        shelf.updated_at = datetime.now()

        # 更新货架的经纬度（如果消息中包含）
        if status_record.longitude and status_record.latitude:
            shelf.longitude = status_record.longitude
            shelf.latitude = status_record.latitude


        db.add(status_record)

        db.commit()
        db.refresh(status_record)
        return status_record
    except Exception as e:
        db.rollback()
        print(f"解析并保存状态失败: {str(e)}")
        return None

# MQTT消息处理回调函数
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("已成功连接到MQTT Broker")
        client.subscribe(MQTT_TOPIC)
        print(f"已订阅主题: {MQTT_TOPIC}")
    else:
        print(f"连接失败，错误代码: {rc}")

def on_message(client, userdata, msg):
    print(f"收到消息 - 主题: {msg.topic}, 时间: {datetime.now()}")
    
    # 获取数据库会话
    db = next(get_db())
    
    try:
        # 保存原始消息
        payload = msg.payload.decode('utf-8')
        shelf_code = None
        
        # 尝试解析消息以获取货架编号
        try:
            message_data = json.loads(payload)
            shelf_code = message_data.get('t')
        except json.JSONDecodeError:
            print("原始消息不是有效的JSON格式")
        
        # 保存原始消息到数据库
        save_raw_message(db, msg.topic, payload, shelf_code)
        
        # 解析并保存状态数据
        try:
            message_data = json.loads(payload)
            parse_and_save_status(db, message_data)
        except json.JSONDecodeError as e:
            print(f"解析消息失败: {str(e)}")
        except Exception as e:
            print(f"处理消息时发生错误: {str(e)}")
            
    finally:
        # 关闭数据库会话
        db.close()

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("意外断开连接")
    print("已断开与MQTT Broker的连接")

def main():

   # 确保数据库表存在
    try:
        Base.metadata.create_all(engine)
        print("数据库表检查完成，所有表已存在或已创建。")
    except Exception as e:
        print(f"创建数据库表时发生错误: {str(e)}")
        return  # 如果表创建失败，程序无法继续，直接退出
    
    db = get_db()

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
        print(f"连接到MQTT Broker失败: {str(e)}")
        return
    
    # 保持连接并处理消息
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("用户中断程序")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()
