import pymysql
import configparser
import datetime
import os
from typing import List, Dict, Optional

# -------------------------- 配置文件路径 --------------------------
CONFIG_FILE_PATH = "confignew.ini"  # 配置文件路径，可根据实际路径调整
TABLE_NAME = "shelf_logs"  # 目标表名
# -------------------------------------------------------------


class ShelfLogsCleaner:
    def __init__(self, minutes_before: int):
        """
        初始化清理器
        :param minutes_before: 距离当前时间之前的分钟数（查询该时间范围内的记录）
        """
        self.minutes_before = minutes_before
        self.conn: Optional[pymysql.connections.Connection] = None
        self.cursor: Optional[pymysql.cursors.DictCursor] = None
        # 内存中存储的基准记录（用于对比）
        self.base_record: Optional[Dict] = None
        # 记录被删除的ID（用于日志和验证）
        self.deleted_ids: List[int] = []
        # 数据库配置
        self.db_config = self.load_db_config()

    def load_db_config(self) -> Dict:
        """
        从confignew.ini读取数据库配置
        :return: 数据库配置字典
        """
        # 检查配置文件是否存在
        if not os.path.exists(CONFIG_FILE_PATH):
            raise FileNotFoundError(f"❌ 配置文件不存在：{CONFIG_FILE_PATH}")

        # 解析配置文件
        config = configparser.ConfigParser()
        try:
            config.read(CONFIG_FILE_PATH, encoding="utf-8")
        except Exception as e:
            raise Exception(f"❌ 读取配置文件失败：{e}")

        # 检查[database]节是否存在
        if "database" not in config.sections():
            raise KeyError(f"❌ 配置文件中未找到[database]节")

        # 提取配置项（带默认值和类型转换）
        try:
            db_config = {
                "host": config.get("database", "host"),
                "port": config.getint("database", "port"),  # 转换为整数
                "user": config.get("database", "user"),
                "password": config.get("database", "password"),
                "database": config.get("database", "name"),  # 配置中是name，对应代码中的database
                "charset": "utf8mb4",
                "cursorclass": pymysql.cursors.DictCursor
            }
            # 校验必填项
            required_keys = ["host", "port", "user", "password", "database"]
            for key in required_keys:
                if not db_config[key]:
                    raise ValueError(f"❌ [database]节中缺少配置项：{key}")
            print(f"✅ 配置文件读取成功 | 数据库地址：{db_config['host']}:{db_config['port']} | 数据库名：{db_config['database']}")
            return db_config
        except configparser.NoOptionError as e:
            raise Exception(f"❌ 配置文件[database]节缺少参数：{e}")
        except ValueError as e:
            raise Exception(f"❌ 配置参数格式错误：{e}")

    def connect_db(self) -> None:
        """连接数据库（使用读取的配置）"""
        try:
            self.conn = pymysql.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print(f"✅ 数据库连接成功 | 时间：{datetime.datetime.now()}")
        except pymysql.MySQLError as e:
            raise Exception(f"❌ 数据库连接失败：{e}")

    def close_db(self) -> None:
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print(f"✅ 数据库连接已关闭 | 时间：{datetime.datetime.now()}")

    def get_target_records(self) -> List[Dict]:
        """
        查询指定时间范围内的记录（按id倒序）
        :return: 符合条件的记录列表（字典格式）
        """
        # 计算时间阈值：当前时间 - 指定分钟数
        time_threshold = datetime.datetime.now() - datetime.timedelta(minutes=self.minutes_before)
        time_threshold_str = time_threshold.strftime("%Y-%m-%d %H:%M:%S")

        # 查询SQL：按id倒序，仅查询指定时间范围内的记录
        query_sql = f"""
            SELECT * FROM {TABLE_NAME} 
            WHERE log_time >= %s 
            ORDER BY id DESC;
        """

        try:
            self.cursor.execute(query_sql, (time_threshold_str,))
            records = self.cursor.fetchall()
            print(f"✅ 查询到 {len(records)} 条记录 | 时间范围：{time_threshold_str} 至 当前时间")
            return records
        except pymysql.MySQLError as e:
            raise Exception(f"❌ 查询记录失败：{e}")

    def calculate_time_diff(self, time1: str, time2: str) -> int:
        """
        计算两个时间字符串的分钟差（绝对值）
        :param time1: 时间字符串（格式：%Y-%m-%d %H:%M:%S）
        :param time2: 时间字符串（格式：%Y-%m-%d %H:%M:%S）
        :return: 分钟差（整数）
        """
        fmt = "%Y-%m-%d %H:%M:%S"
        t1 = datetime.datetime.strptime(time1, fmt)
        t2 = datetime.datetime.strptime(time2, fmt)
        diff_seconds = abs((t1 - t2).total_seconds())
        return int(diff_seconds // 60)

    def delete_record(self, record_id: int) -> None:
        """
        删除指定ID的记录
        :param record_id: 记录ID
        """
        delete_sql = f"DELETE FROM {TABLE_NAME} WHERE id = %s;"
        try:
            self.cursor.execute(delete_sql, (record_id,))
            self.conn.commit()
            self.deleted_ids.append(record_id)
            print(f"✅ 删除记录成功 | ID：{record_id}")
        except pymysql.MySQLError as e:
            self.conn.rollback()
            raise Exception(f"❌ 删除记录失败（ID：{record_id}）：{e}")

    def process_records(self, records: List[Dict]) -> None:
        """
        遍历记录并处理：对比shelf_id、current_quantity、时间差，删除符合条件的记录
        :param records: 查询到的记录列表
        """
        if not records:
            print("⚠️ 无记录需要处理")
            return

        # 初始化基准记录（第一个记录）
        self.base_record = records[0]
        print(f"🔹 初始化基准记录 | ID：{self.base_record['id']} | shelf_id：{self.base_record['shelf_id']} "
              f"| current_quantity：{self.base_record['current_quantity']} | log_time：{self.base_record['log_time']}")

        # 遍历剩余记录（从第二个开始）
        for idx in range(1, len(records)):
            current_record = records[idx]
            print(f"\n🔹 处理第 {idx+1} 条记录 | ID：{current_record['id']} | shelf_id：{current_record['shelf_id']} "
                  f"| current_quantity：{current_record['current_quantity']} | log_time：{current_record['log_time']}")

            # 对比条件1：shelf_id 相同
            if current_record['shelf_id'] != self.base_record['shelf_id']:
                print(f"⚠️ shelf_id 不匹配（基准：{self.base_record['shelf_id']}，当前：{current_record['shelf_id']}），更新基准记录")
                self.base_record = current_record
                continue

            # 对比条件2：current_quantity 相同
            if current_record['current_quantity'] != self.base_record['current_quantity']:
                print(f"⚠️ current_quantity 不匹配（基准：{self.base_record['current_quantity']}，当前：{current_record['current_quantity']}），更新基准记录")
                self.base_record = current_record
                continue

            # 对比条件3：时间差不超过2分钟
            time_diff = self.calculate_time_diff(
                str(self.base_record['log_time']),
                str(current_record['log_time'])
            )
            print(f"🔍 时间差：{time_diff} 分钟")

            if time_diff <= 2:
                print(f"⚠️ 符合删除条件（时间差≤2分钟），执行删除")
                self.delete_record(current_record['id'])
            else:
                print(f"⚠️ 时间差超过2分钟，更新基准记录")
                self.base_record = current_record

    def run(self) -> None:
        """主执行流程"""
        try:
            # 1. 连接数据库
            self.connect_db()
            # 2. 查询目标记录
            records = self.get_target_records()
            # 3. 处理记录
            self.process_records(records)
            # 4. 输出最终结果
            print(f"\n==================== 处理完成 ====================")
            print(f"📊 总计查询记录数：{len(records)}")
            print(f"🗑️ 总计删除记录数：{len(self.deleted_ids)}")
            if self.deleted_ids:
                print(f"🆔 被删除的记录ID：{', '.join(map(str, self.deleted_ids))}")
        except Exception as e:
            print(f"\n❌ 程序执行失败：{e}")
        finally:
            # 无论是否异常，都关闭数据库连接
            self.close_db()


if __name__ == "__main__":
    # -------------------------- 自定义参数 --------------------------
    # 指定距离当前时间之前的分钟数（例如：61分钟，对应需求中的61分钟范围）
    TARGET_MINUTES_BEFORE = 6100
    # -------------------------------------------------------------

    # 初始化并运行清理器
    try:
        cleaner = ShelfLogsCleaner(minutes_before=TARGET_MINUTES_BEFORE)
        cleaner.run()
    except Exception as e:
        print(f"❌ 程序初始化失败：{e}")