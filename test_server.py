# test_server.py
import requests
import struct
import time
from datetime import datetime, timedelta, timezone

# --- 配置 ---
# 确保你的 Rust 服务器正在运行
SERVER_URL = "http://127.0.0.1:3000"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 10 # 请求10条，以确保覆盖5分钟

# --- 二进制格式常量 (必须与 transformer.rs 中的定义完全一致) ---
MAX_KLINE_RECORDS = 2000
NUM_FIELDS = 9
FIELD_BLOCK_SIZE = MAX_KLINE_RECORDS * 8  # 2000 * sizeof(f64/i64)
KLINE_DATA_BODY_OFFSET = 48 # 全局头(8) + 记录头(32) + K线头(8)

# 字段在二进制布局中的顺序 (Struct of Arrays)
# 对应 transformer.rs 中的写入顺序
FIELD_ORDER = [
    "close", "ext_long_short_ratio", "ext_open_interest", "ext_funding_rate",
    "high", "low", "open", "timestamp", "quote_volume"
]
# 字段的数据类型
# d = f64 (double), q = i64 (long long)
FIELD_TYPES = ['d', 'd', 'd', 'd', 'd', 'd', 'd', 'q', 'd']


# 【新增】辅助函数，用于将毫秒时间戳转换为可读的UTC时间字符串
def ms_to_readable_utc(ms: int) -> str:
    """将毫秒时间戳转换为 'YYYY-MM-DD HH:MM:SS UTC' 格式的字符串"""
    dt_object = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt_object.strftime('%Y-%m-%d %H:%M:%S UTC')


def parse_kline_blob(data: bytes):
    """
    解析从服务器收到的二进制 K 线数据 blob
    """
    print(f"\n--- 开始解析二进制数据 (收到 {len(data)} 字节) ---\n")

    if len(data) < KLINE_DATA_BODY_OFFSET:
        print(f"错误：收到的数据大小不足 {KLINE_DATA_BODY_OFFSET} 字节，无法解析。")
        return None

    # 1. 解析全局头部 (8字节)
    record_count, reserved = struct.unpack_from('<II', data, 0)
    print(f"全局头部:")
    print(f"  - 记录数量 (Record Count): {record_count}")
    print(f"  - 保留字段 (Reserved): {reserved}")

    # 2. 解析记录体头部 (32字节)
    symbol_period_bytes = struct.unpack_from('<32s', data, 8)[0]
    symbol_period_str = symbol_period_bytes.decode('utf-8').strip('\x00')
    print(f"\n记录体头部:")
    print(f"  - 品种周期字符串: '{symbol_period_str}'")

    # 3. 解析K线数据缓冲区头部 (8字节)
    kline_count, start_index = struct.unpack_from('<II', data, 40)
    print(f"\nK线数据头部:")
    print(f"  - 实际K线数量 (Kline Count): {kline_count}")
    print(f"  - 起始索引 (Start Index): {start_index}")

    if kline_count == 0:
        print("\nK线数量为0，无需解析数据体。")
        return {"kline_count": 0, "klines": []}
    
    # 4. 解析K线数据体 (Struct of Arrays 布局)
    print("\n--- 解析前5条K线数据 ---")
    klines = []
    for i in range(min(kline_count, 5)): # 最多只打印前5条
        kline_data = {}
        for j, field_name in enumerate(FIELD_ORDER):
            field_type = FIELD_TYPES[j]
            offset = KLINE_DATA_BODY_OFFSET + (j * FIELD_BLOCK_SIZE) + (i * 8)
            value = struct.unpack_from(f'<{field_type}', data, offset)[0]
            kline_data[field_name] = value
        klines.append(kline_data)

    for i, k in enumerate(klines):
        ts_ms = k['timestamp']
        readable_time = ms_to_readable_utc(ts_ms) # 【修改】使用新的辅助函数
        
        print(f"\n[K线 #{i+1}]")
        print(f"  - Time:      {readable_time} ({ts_ms})")
        print(f"  - Open:      {k['open']:.4f}")
        print(f"  - High:      {k['high']:.4f}")
        print(f"  - Low:       {k['low']:.4f}")
        print(f"  - Close:     {k['close']:.4f}")
        print(f"  - Volume:    {k['quote_volume']:.2f}")

    return {"kline_count": kline_count, "klines": klines, "symbol": symbol_period_str}


def run_test():
    """
    主测试函数
    """
    # 1. 计算5分钟前的时间戳 (毫秒)
    now = datetime.now(timezone.utc)
    five_minutes_ago = now - timedelta(minutes=5)
    start_time_ms = int(five_minutes_ago.timestamp() * 1000)

    # 2. 构造请求 URL
    url = f"{SERVER_URL}/download-binary/{SYMBOL}/{INTERVAL}?limit={LIMIT}&startTime={start_time_ms}"

    print("="*50)
    print("Binance Local Server - Python Test Client")
    print("="*50)
    print(f"发起请求到:")
    print(f"  URL: {url}")
    # 【修改】使用新的辅助函数格式化 startTime
    print(f"  (startTime 对应 {ms_to_readable_utc(start_time_ms)})")

    # 3. 发送请求
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"\n[测试失败] 请求出错: {e}")
        return

    # 4. 解析并验证响应
    print(f"\n请求成功! 状态码: {response.status_code}")
    binary_data = response.content
    parsed_result = parse_kline_blob(binary_data)
    
    if parsed_result is None:
        print("\n[测试失败] 二进制数据解析失败。")
        return

    # 5. 最终验证
    print("\n--- 最终验证 ---")
    kline_count = parsed_result["kline_count"]
    klines = parsed_result["klines"]
    
    if not (5 <= kline_count <= 7):
        print(f"🟡 [警告] K线数量为 {kline_count}，预期为5或6。")
    else:
        print(f"✅ [通过] K线数量 ({kline_count}) 在预期范围内。")

    # 【修改】对验证部分的 open_time 进行格式化输出
    if klines:
        first_kline_time = klines[0]['timestamp']
        if first_kline_time >= start_time_ms:
            start_time_str = ms_to_readable_utc(start_time_ms)
            first_kline_time_str = ms_to_readable_utc(first_kline_time)
            print(f"✅ [通过] 第一根K线的 open_time >= 请求的 startTime。")
            print(f"     ├─ K线 Open Time: {first_kline_time_str} ({first_kline_time})")
            print(f"     └─ 请求 StartTime: {start_time_str} ({start_time_ms})")
        else:
            start_time_str = ms_to_readable_utc(start_time_ms)
            first_kline_time_str = ms_to_readable_utc(first_kline_time)
            print(f"❌ [失败] 第一根K线的 open_time < 请求的 startTime。")
            print(f"     ├─ K线 Open Time: {first_kline_time_str} ({first_kline_time})")
            print(f"     └─ 请求 StartTime: {start_time_str} ({start_time_ms})")
    
    print("\n[测试完成]")


if __name__ == "__main__":
    run_test()