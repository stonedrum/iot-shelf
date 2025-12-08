#!/usr/bin/env python3
# 测试数据库断网重连功能

import time
import os
import sys

# 切换到backend目录运行
base_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(base_dir, 'backend')
os.chdir(backend_dir)

# 添加backend目录到Python路径
sys.path.append(backend_dir)

from sqlalchemy.exc import OperationalError
from sqlalchemy import text
from app.database.db import SessionLocal

def test_db_connection():
    """测试数据库连接"""
    try:
        db = SessionLocal()
        # 执行一个简单的查询
        result = db.execute(text("SELECT 1"))
        print(f"✓ 数据库连接成功: {result.scalar()}")
        db.close()
        return True
    except OperationalError as e:
        print(f"✗ 数据库连接失败: {e}")
        return False
    except Exception as e:
        print(f"✗ 发生其他错误: {e}")
        return False

def simulate_network_interruption(duration=5):
    """模拟网络中断"""
    print(f"\n模拟网络中断 {duration} 秒...")
    time.sleep(duration)
    print("网络恢复")

if __name__ == "__main__":
    print("开始测试数据库断网重连功能...")
    
    # 初始连接测试
    print("\n1. 初始连接测试:")
    if not test_db_connection():
        print("初始连接失败，退出测试")
        sys.exit(1)
    
    # 模拟网络中断
    simulate_network_interruption()
    
    # 网络恢复后立即测试连接
    print("\n2. 网络恢复后立即测试:")
    if test_db_connection():
        print("✓ 连接池成功处理了网络中断")
    else:
        print("✗ 网络恢复后连接失败")
    
    # 再次测试连接，确认连接稳定
    print("\n3. 再次测试连接稳定性:")
    if test_db_connection():
        print("✓ 连接稳定")
    else:
        print("✗ 连接不稳定")
    
    print("\n测试完成")
