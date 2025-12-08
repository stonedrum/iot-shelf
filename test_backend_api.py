import requests
import json
import datetime

# 后端API基础URL
BASE_URL = "http://localhost:8000"

# 测试数据
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "123456"

# 测试结果统计
test_results = {
    "total": 0,
    "passed": 0,
    "failed": 0,
    "errors": []
}

def print_result(test_name, status, message=None):
    """打印测试结果"""
    global test_results
    test_results["total"] += 1
    if status == "PASS":
        test_results["passed"] += 1
        print(f"✅ {test_name}: PASS")
    else:
        test_results["failed"] += 1
        test_results["errors"].append(f"❌ {test_name}: FAIL - {message}")
        print(f"❌ {test_name}: FAIL - {message}")

def test_login():
    """测试登录API"""
    test_name = "Login API"
    try:
        response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD}
        )
        if response.status_code == 200:
            data = response.json()
            if "access_token" in data and "user" in data:
                print_result(test_name, "PASS")
                return data["access_token"]
            else:
                print_result(test_name, "FAIL", "Missing access_token or user in response")
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_get_cities(token):
    """测试获取城市列表API"""
    test_name = "Get Cities API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/cities", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_get_stations(token):
    """测试获取站点列表API"""
    test_name = "Get Stations API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/stations", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_get_shelves(token):
    """测试获取货架列表API"""
    test_name = "Get Shelves API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelves", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_get_users(token):
    """测试获取用户列表API"""
    test_name = "Get Users API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/users", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_user_cities(token):
    """测试获取用户城市列表API"""
    test_name = "Get User Cities API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/users/current/cities", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_get_shelf_logs(token):
    """测试获取货架日志列表API"""
    test_name = "Get Shelf Logs API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelf-logs", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_get_shelf_statistics(token):
    """测试获取货架统计数据API"""
    test_name = "Get Shelf Statistics API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelves/statistics", headers=headers)
        if response.status_code == 200:
            data = response.json()
            # 检查返回的数据是否包含所有必要的统计字段
            required_fields = ["total_shelves", "offline_shelves", "total_quantity", "today_new_shelves", "low_quantity_shelves", "low_voltage_shelves", "delivering_shelves", "total_cities", "total_stations", "expired_sim_shelves", "low_signal_shelves"]
            for field in required_fields:
                if field not in data:
                    print_result(test_name, "FAIL", f"Missing required field: {field}")
                    return None
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_get_offline_shelves(token):
    """测试获取离线货架列表API"""
    test_name = "Get Offline Shelves API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelves/offline", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_get_low_quantity_shelves(token):
    """测试获取低存量货架列表API"""
    test_name = "Get Low Quantity Shelves API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelves/low-quantity", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_get_low_voltage_shelves(token):
    """测试获取低电压货架列表API"""
    test_name = "Get Low Voltage Shelves API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelves/low-voltage", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_get_delivering_shelves(token):
    """测试获取发货中货架列表API"""
    test_name = "Get Delivering Shelves API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelves/delivering", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_get_expired_sim_shelves(token):
    """测试获取流量卡到期的货架列表API"""
    test_name = "Get Expired SIM Shelves API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelves/expired-sim", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_get_low_signal_shelves(token):
    """测试获取信号强度低的货架列表API"""
    test_name = "Get Low Signal Shelves API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelves/low-signal", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return []

def test_pagination(token):
    """测试所有列表接口的分页功能"""
    pagination_apis = [
        ("Cities Pagination", "/api/cities"),
        ("Stations Pagination", "/api/stations"),
        ("Shelves Pagination", "/api/shelves"),
        ("Users Pagination", "/api/users"),
        ("Shelf Logs Pagination", "/api/shelf-logs")
    ]
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # 测试不同的分页参数
    pagination_params = [
        {"skip": 0, "limit": 5},
        {"skip": 5, "limit": 10},
        {"skip": 0, "limit": 100}  # 测试最大限制
    ]
    
    all_passed = True
    
    for api_name, api_path in pagination_apis:
        for params in pagination_params:
            test_name = f"{api_name} (skip={params['skip']}, limit={params['limit']})"
            try:
                response = requests.get(f"{BASE_URL}{api_path}", headers=headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                    # 验证返回的数据量不超过limit
                    if len(data) <= params['limit']:
                        print_result(test_name, "PASS")
                    else:
                        print_result(test_name, "FAIL", f"返回的数据量({len(data)})超过了limit({params['limit']})")
                        all_passed = False
                else:
                    print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
                    all_passed = False
            except Exception as e:
                print_result(test_name, "FAIL", f"Exception: {str(e)}")
                all_passed = False
    
    return all_passed

def test_create_city(token):
    """测试创建城市API"""
    test_name = "Create City API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        new_city = {
            "city_name": "Test City",
            "province": "Test Province",
            "region": "Test Region",
            "created_by": "admin",
            "updated_by": "admin"
        }
        response = requests.post(f"{BASE_URL}/api/cities", json=new_city, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data["id"]
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_create_station(token, city_id):
    """测试创建站点API"""
    test_name = "Create Station API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        new_station = {
            "station_name": "Test Station",
            "city_id": city_id,
            "remark": "Test Remark",
            "contact_name": "Test Contact",
            "contact_phone": "13800138000",
            "created_by": "admin",
            "updated_by": "admin"
        }
        response = requests.post(f"{BASE_URL}/api/stations", json=new_station, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data["id"]
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_create_shelf(token, station_id):
    """测试创建货架API"""
    test_name = "Create Shelf API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        new_shelf = {
            "station_id": station_id,
            "iccid": f"ICCID-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}",
            "wechat": "test_wechat",
            "phone": "13800138000",
            "address": "Test Address",
            "total_quantity": 10,
            "product_name": "Test Product",
            "order_quantity": 5,
            "sim_card_number": "13800138000",
            "sim_card_expiry": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            "created_by": "admin",
            "updated_by": "admin"
        }
        response = requests.post(f"{BASE_URL}/api/shelves", json=new_shelf, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data["id"]
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_get_city_detail(token, city_id):
    """测试获取城市详情API"""
    test_name = "Get City Detail API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/cities/{city_id}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_get_station_detail(token, station_id):
    """测试获取站点详情API"""
    test_name = "Get Station Detail API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/stations/{station_id}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_get_shelf_detail(token, shelf_id):
    """测试获取货架详情API"""
    test_name = "Get Shelf Detail API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelves/{shelf_id}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_get_shelf_logs_by_shelf(token, shelf_id):
    """测试获取特定货架的日志API"""
    test_name = "Get Shelf Logs by Shelf API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{BASE_URL}/api/shelf-logs/shelf/{shelf_id}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_create_user(token):
    """测试创建用户API"""
    test_name = "Create User API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        new_user = {
            "username": f"test_user_{timestamp}",
            "password": "123456",
            "phone": f"1380013{timestamp[-4:]}",  # 使用时间戳的后4位保证唯一性
            "role": "general",
            "created_by": "admin",
            "updated_by": "admin"
        }
        response = requests.post(f"{BASE_URL}/api/users", json=new_user, headers=headers, params={"city_ids": [1]})
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data["id"]
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_update_user(token, user_id):
    """测试编辑用户API"""
    test_name = "Update User API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        # 修改用户的用户名，避免电话号码冲突
        update_data = {
            "username": f"test_user_updated_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}",
            "updated_by": "admin"
        }
        response = requests.put(f"{BASE_URL}/api/users/{user_id}", json=update_data, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_reset_user_password(token, user_id):
    """测试重置用户密码API"""
    test_name = "Reset User Password API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(f"{BASE_URL}/api/users/{user_id}/reset-password", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_update_city(token, city_id):
    """测试编辑城市API"""
    test_name = "Update City API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        update_data = {
            "city_name": "Updated City",
            "province": "Updated Province",
            "region": "Updated Region",
            "updated_by": "admin"
        }
        response = requests.put(f"{BASE_URL}/api/cities/{city_id}", json=update_data, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_update_station(token, station_id):
    """测试编辑站点API"""
    test_name = "Update Station API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        update_data = {
            "station_name": "Updated Station",
            "remark": "Updated Remark",
            "contact_name": "Updated Contact",
            "contact_phone": "13800138003",
            "updated_by": "admin"
        }
        response = requests.put(f"{BASE_URL}/api/stations/{station_id}", json=update_data, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def test_update_shelf(token, shelf_id):
    """测试编辑货架API"""
    test_name = "Update Shelf API"
    try:
        headers = {"Authorization": f"Bearer {token}"}
        update_data = {
            "phone": "13800138004",
            "address": "Updated Address",
            "total_quantity": 20,
            "order_quantity": 10,
            "updated_by": "admin"
        }
        response = requests.put(f"{BASE_URL}/api/shelves/{shelf_id}", json=update_data, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print_result(test_name, "PASS")
            return data
        else:
            print_result(test_name, "FAIL", f"Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print_result(test_name, "FAIL", f"Exception: {str(e)}")
    return None

def main():
    """主测试函数"""
    print("=" * 60)
    print("后端API测试脚本")
    print("=" * 60)
    print(f"测试时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API基础URL: {BASE_URL}")
    print("=" * 60)
    
    # 1. 测试登录API
    token = test_login()
    
    if token:
        print("\n" + "=" * 60)
        print("认证成功，开始测试其他API...")
        print("=" * 60)
        
        # 2. 测试获取列表API
        print("\n--- 测试获取列表API ---")
        test_get_cities(token)
        test_get_stations(token)
        test_get_shelves(token)
        test_get_users(token)
        test_get_shelf_logs(token)
        
        # 测试新增的统计和特殊状态列表API
        print("\n--- 测试新增的统计和特殊状态列表API ---")
        test_get_shelf_statistics(token)
        test_get_offline_shelves(token)
        test_get_low_quantity_shelves(token)
        test_get_low_voltage_shelves(token)
        test_get_delivering_shelves(token)
        test_get_expired_sim_shelves(token)
        test_get_low_signal_shelves(token)
        
        # 测试分页功能
        print("\n--- 测试分页功能 ---")
        test_pagination(token)
        
        # 测试用户城市列表接口
        print("\n--- 测试用户城市列表接口 ---")
        test_user_cities(token)
        
        # 3. 测试创建资源API
        city_id = test_create_city(token)
        user_id = None
        
        if city_id:
            station_id = test_create_station(token, city_id)
            user_id = test_create_user(token)
            
            if station_id:
                shelf_id = test_create_shelf(token, station_id)
                
                if shelf_id:
                    # 4. 测试获取详情API
                    test_get_city_detail(token, city_id)
                    test_get_station_detail(token, station_id)
                    test_get_shelf_detail(token, shelf_id)
                    test_get_shelf_logs_by_shelf(token, shelf_id)
                    
                    # 5. 测试更新资源API
                    test_update_city(token, city_id)
                    test_update_station(token, station_id)
                    test_update_shelf(token, shelf_id)
                    
                    # 6. 测试更新用户API
                    if user_id:
                        test_update_user(token, user_id)
                        # 7. 测试重置用户密码API
                        test_reset_user_password(token, user_id)
    
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    print(f"总测试数: {test_results['total']}")
    print(f"通过数: {test_results['passed']}")
    print(f"失败数: {test_results['failed']}")
    
    if test_results['errors']:
        print("\n失败详情:")
        for error in test_results['errors']:
            print(f"  {error}")
    
    print("\n" + "=" * 60)
    if test_results['failed'] == 0:
        print("🎉 所有API测试通过！")
    else:
        print("⚠️  部分API测试失败，请检查后端服务和API实现。")
    print("=" * 60)

if __name__ == "__main__":
    main()