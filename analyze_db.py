# analyze_db.py

import sqlite3
import pandas as pd
from typing import List, Tuple

DB_PATH = "kline_cache.db"
TABLE_NAME = "klines"

def analyze_kline_database(db_path: str) -> None:
    """
    连接到 SQLite 数据库，分析并打印每个交易对/周期的K线数量。

    :param db_path: SQLite 数据库文件路径
    """
    print(f"--- Analyzing Kline Database: {db_path} ---")

    try:
        # 1. 连接到数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("✅ Database connection successful.")

        # 2. 检查 'klines' 表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (TABLE_NAME,))
        if cursor.fetchone() is None:
            print(f"❌ Error: Table '{TABLE_NAME}' not found in the database.")
            return

        # 3. 执行 SQL 查询以统计每个组合的数量
        # GROUP BY symbol, interval 会对每个唯一的组合进行分组
        # COUNT(*) 会计算每个组内的行数（即K线数量）
        query = f"SELECT symbol, interval, COUNT(*) as kline_count FROM {TABLE_NAME} GROUP BY symbol, interval ORDER BY symbol, interval;"
        
        print(f"\n🔍 Executing query: {query}")
        
        cursor.execute(query)
        results: List[Tuple[str, str, int]] = cursor.fetchall()

        # 4. 关闭数据库连接
        conn.close()
        print("✅ Database connection closed.")
        
        if not results:
            print("\nDatabase contains no k-line data.")
            return

        # 5. 使用 pandas 来格式化和展示结果
        # 创建一个 DataFrame
        df = pd.DataFrame(results, columns=['Symbol', 'Interval', 'Kline Count'])
        
        # 打印结果表格
        print("\n📊 Kline Count Summary:")
        print(df.to_string(index=False)) # to_string() 提供了更好的格式

        # 打印一些总计信息
        total_symbols = df['Symbol'].nunique()
        total_combinations = len(df)
        total_klines = df['Kline Count'].sum()
        
        print("\n--- Totals ---")
        print(f"Total Unique Symbols: {total_symbols}")
        print(f"Total (Symbol, Interval) Combinations: {total_combinations}")
        print(f"Total K-lines in DB: {total_klines:,}") # 使用逗号分隔符

    except sqlite3.Error as e:
        print(f"\n❌ A database error occurred: {e}")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    analyze_kline_database(DB_PATH)