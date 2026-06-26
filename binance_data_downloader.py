"""
Binance SOLUSDT 历史数据下载器
================================
使用 CCXT 库下载 Binance 交易所 SOL/USDT 永续合约的 K 线数据及所有衍生数据。

数据范围: 最新 60 天
K线周期: 5m, 15m, 30m, 1h, 2h
衍生数据: 资金费率历史、未平仓合约历史、多空比历史、Ticker、深度快照、最近成交
保存路径: ./data/

防封机制:
  - CCXT 内置速率限制器 (enableRateLimit)
  - 自定义请求间隔 (rateLimit=500ms)
  - 指数退避重试 (最多5次)
  - 权重监控与自适应降速
  - 请求计数与进度日志

依赖安装: pip install ccxt pandas

使用方法: python binance_data_downloader.py
"""

import ccxt
import pandas as pd
import os
import time
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ============================================================
# 配置区域
# ============================================================
SYMBOL = "SOL/USDT"
TIMEFRAMES = ["5m", "15m", "30m", "1h", "2h"]
DAYS = 60
DATA_DIR = Path(__file__).parent / "data"

# 防封配置
RATE_LIMIT_MS = 500           # 请求间隔 (毫秒), 越大越安全
MAX_RETRIES = 5               # 最大重试次数
RETRY_BACKOFF_BASE = 2.0      # 退避基数 (秒)
WEIGHT_THRESHOLD = 1800        # 权重阈值 (合约2400/分钟), 到达后暂停
WEIGHT_COOLDOWN_SEC = 30       # 权重冷却时间 (秒)

# ============================================================
# 日志配置
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(DATA_DIR / "download.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ============================================================
# 防封机制核心类
# ============================================================
class BinanceAntiBan:
    """Binance API 防封机制管理器"""

    def __init__(self, exchange: ccxt.Exchange):
        self.exchange = exchange
        self.request_count = 0
        self.start_time = time.time()
        self.minute_weights = {}  # timestamp_minute -> used_weight

    def get_used_weight(self, response) -> int:
        """从响应头提取已使用权重"""
        try:
            return int(response.headers.get("X-MBX-USED-WEIGHT-1M", 0))
        except (AttributeError, ValueError, TypeError):
            return 0

    def check_weight_and_throttle(self, response=None, weight_cost=1):
        """检查权重并自适应降速"""
        self.request_count += 1

        # 从响应头获取权重
        if response is not None:
            used_weight = self.get_used_weight(response)
            current_minute = int(time.time() // 60)
            self.minute_weights[current_minute] = used_weight

            # 清理过期的权重记录
            expired = [k for k in self.minute_weights if current_minute - k > 1]
            for k in expired:
                del self.minute_weights[k]

            # 当前分钟权重
            current_weight = self.minute_weights.get(current_minute, 0)
            if current_weight >= WEIGHT_THRESHOLD:
                logger.warning(
                    f"权重接近上限 ({current_weight}/{WEIGHT_THRESHOLD}), "
                    f"冷却 {WEIGHT_COOLDOWN_SEC} 秒..."
                )
                time.sleep(WEIGHT_COOLDOWN_SEC)

        # 额外的安全间隔
        if self.request_count % 50 == 0:
            elapsed = time.time() - self.start_time
            avg_interval = (elapsed / self.request_count) * 1000
            if avg_interval < RATE_LIMIT_MS * 0.8:
                logger.info(f"自适应降速: 平均间隔 {avg_interval:.0f}ms < {RATE_LIMIT_MS * 0.8:.0f}ms, 额外等待1秒")
                time.sleep(1)

    def retry_with_backoff(self, func, *args, **kwargs):
        """指数退避重试"""
        last_exception = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = func(*args, **kwargs)
                return result
            except ccxt.RateLimitExceeded as e:
                last_exception = e
                wait = RETRY_BACKOFF_BASE ** attempt
                logger.warning(f"触发速率限制, 第 {attempt}/{MAX_RETRIES} 次重试, 等待 {wait:.1f}s")
                time.sleep(wait)
            except ccxt.NetworkError as e:
                last_exception = e
                wait = RETRY_BACKOFF_BASE ** attempt
                logger.warning(f"网络错误, 第 {attempt}/{MAX_RETRIES} 次重试, 等待 {wait:.1f}s: {e}")
                time.sleep(wait)
            except ccxt.ExchangeError as e:
                last_exception = e
                if "banned" in str(e).lower() or "temporarily" in str(e).lower():
                    wait = RETRY_BACKOFF_BASE ** attempt * 2
                    logger.error(f"可能被封禁! 第 {attempt}/{MAX_RETRIES} 次重试, 等待 {wait:.1f}s")
                    time.sleep(wait)
                else:
                    raise
        logger.error(f"重试 {MAX_RETRIES} 次后仍失败: {last_exception}")
        raise last_exception

    def stats(self):
        """请求统计"""
        elapsed = time.time() - self.start_time
        return {
            "total_requests": self.request_count,
            "elapsed_seconds": round(elapsed, 1),
            "avg_interval_ms": round((elapsed / max(self.request_count, 1)) * 1000, 1),
        }


# ============================================================
# 数据下载函数
# ============================================================

def create_exchange():
    """创建 CCXT Binance 实例 (含防封配置 + GitHub Actions 兼容)"""
    is_github_actions = os.environ.get("GITHUB_ACTIONS") == "true"

    config = {
        "enableRateLimit": True,
        "rateLimit": RATE_LIMIT_MS,
        "timeout": 30000,
        "options": {
            "adjustForTimeDifference": True,
        },
    }

    if is_github_actions:
        # GitHub Actions runner 在美国，Binance 对美国 IP 有限制
        # 使用 data-api.binance.vision (币安公开数据 API，不受地域限制)
        logger.info("检测到 GitHub Actions 环境，使用 data-api.binance.vision")
        config["urls"] = {
            "api": {
                "public": "https://data-api.binance.vision/api",
                "private": "https://data-api.binance.vision/api",
            }
        }
        config["options"]["defaultType"] = "spot"
        market_type = "spot"
    else:
        config["options"]["defaultType"] = "future"
        market_type = "future"

    exchange = ccxt.binance(config)

    if is_github_actions:
        # data-api.binance.vision 不支持 exchangeInfo 端点，手动注入 markets
        sol_usdt_market = {
            "id": "SOLUSDT",
            "symbol": "SOL/USDT",
            "base": "SOL",
            "quote": "USDT",
            "baseId": "SOL",
            "quoteId": "USDT",
            "type": "spot",
            "spot": True,
            "margin": False,
            "swap": False,
            "future": False,
            "option": False,
            "active": True,
            "contract": False,
            "precision": {
                "amount": 4,
                "price": 4,
                "cost": None,
                "base": 8,
                "quote": 8,
            },
            "limits": {
                "leverage": {"min": None, "max": None},
                "amount": {"min": 0.0001, "max": 9000.0},
                "price": {"min": 0.0001, "max": 100000.0},
                "cost": {"min": 1.0, "max": None},
            },
            "info": {"symbol": "SOLUSDT", "status": "TRADING"},
        }
        exchange.markets = {"SOL/USDT": sol_usdt_market}
        exchange.markets_by_id = {"SOLUSDT": sol_usdt_market}
        exchange.symbols = ["SOL/USDT"]
        exchange.ids = ["SOLUSDT"]
        exchange.currencies = {
            "SOL": {"id": "SOL", "code": "SOL", "name": "Solana", "active": True},
            "USDT": {"id": "USDT", "code": "USDT", "name": "Tether", "active": True},
        }
        logger.info("手动注入 markets (跳过 exchangeInfo): SOL/USDT")
    else:
        exchange.load_markets()

    logger.info(
        f"交易所初始化完成: {exchange.id} "
        f"(市场数: {len(exchange.markets)}, 类型: {market_type})"
    )
    return exchange


def fetch_all_ohlcv(exchange, symbol, timeframe, since_ms, until_ms, anti_ban):
    """
    分页下载全部 K 线数据
    Binance 单次最多返回 1500 条 (永续合约)
    """
    all_data = []
    cursor = since_ms
    limit = 1500  # Binance 永续合约 klines 最大 limit

    logger.info(f"开始下载 {symbol} {timeframe} K线, 从 {datetime.fromtimestamp(since_ms/1000)} 到 {datetime.fromtimestamp(until_ms/1000)}")

    while cursor < until_ms:
        try:
            ohlcv = anti_ban.retry_with_backoff(
                exchange.fetch_ohlcv,
                symbol, timeframe,
                since=cursor,
                limit=limit,
            )
            anti_ban.check_weight_and_throttle(ohlcv)

            if not ohlcv:
                logger.info(f"{symbol} {timeframe}: 无更多数据, 完成")
                break

            # 过滤掉超出 until 的数据
            filtered = [c for c in ohlcv if c[0] <= until_ms]
            all_data.extend(filtered)

            if len(ohlcv) < limit:
                logger.info(f"{symbol} {timeframe}: 已获取全部数据")
                break

            # 游标移到最后一条K线时间 + 1ms
            cursor = ohlcv[-1][0] + 1

            # 进度日志
            if len(all_data) % 5000 == 0:
                logger.info(f"{symbol} {timeframe}: 已下载 {len(all_data)} 条 K线...")

        except Exception as e:
            logger.error(f"{symbol} {timeframe} 下载失败: {e}")
            break

    logger.info(f"{symbol} {timeframe} K线下载完成: 共 {len(all_data)} 条")
    return all_data


def fetch_funding_rate_history(exchange, symbol, since_ms, until_ms, anti_ban):
    """下载资金费率历史"""
    logger.info(f"开始下载 {symbol} 资金费率历史...")
    all_data = []
    cursor = since_ms
    limit = 1000

    while cursor < until_ms:
        try:
            data = anti_ban.retry_with_backoff(
                exchange.fetch_funding_rate_history,
                symbol,
                since=cursor,
                limit=limit,
            )

            if not data:
                break

            filtered = [d for d in data if d["timestamp"] <= until_ms]
            all_data.extend(filtered)

            if len(data) < limit:
                break

            cursor = data[-1]["timestamp"] + 1
            time.sleep(anti_ban.exchange.rateLimit / 1000)

        except Exception as e:
            logger.error(f"资金费率下载失败: {e}")
            break

    logger.info(f"资金费率历史下载完成: 共 {len(all_data)} 条")
    return all_data


def fetch_open_interest_history(exchange, symbol, timeframe, since_ms, until_ms, anti_ban):
    """下载未平仓合约 (OI) 历史"""
    logger.info(f"开始下载 {symbol} 未平仓合约历史...")
    all_data = []
    cursor = since_ms
    limit = 500  # Binance OI 历史限制较严格

    while cursor < until_ms:
        try:
            data = anti_ban.retry_with_backoff(
                exchange.fetch_open_interest_history,
                symbol, timeframe,
                since=cursor,
                limit=limit,
            )

            if not data:
                break

            filtered = [d for d in data if d["timestamp"] <= until_ms]
            all_data.extend(filtered)

            if len(data) < limit:
                break

            cursor = data[-1]["timestamp"] + 1
            # OI 请求权重较高, 额外等待
            time.sleep(1)

        except Exception as e:
            logger.error(f"未平仓合约下载失败: {e}")
            break

    logger.info(f"未平仓合约历史下载完成: 共 {len(all_data)} 条")
    return all_data


def fetch_long_short_ratio_history(exchange, symbol, timeframe, since_ms, until_ms, anti_ban):
    """下载多空账户比/多空持仓比历史"""
    logger.info(f"开始下载 {symbol} 多空比历史...")
    all_data_ls_account = []
    all_data_ls_position = []
    cursor = since_ms
    limit = 500

    while cursor < until_ms:
        try:
            # 账户多空比 (longShortAccount)
            data_acc = anti_ban.retry_with_backoff(
                exchange.fetch_long_short_ratio_history,
                symbol, timeframe,
                since=cursor,
                limit=limit,
                params={"type": "account"},
            )

            if data_acc:
                filtered = [d for d in data_acc if d["timestamp"] <= until_ms]
                all_data_ls_account.extend(filtered)

            # 持仓多空比 (longShortPosition)
            data_pos = anti_ban.retry_with_backoff(
                exchange.fetch_long_short_ratio_history,
                symbol, timeframe,
                since=cursor,
                limit=limit,
                params={"type": "position"},
            )

            if data_pos:
                filtered = [d for d in data_pos if d["timestamp"] <= until_ms]
                all_data_ls_position.extend(filtered)

            if not data_acc and not data_pos:
                break

            last_ts = 0
            if data_acc:
                last_ts = max(last_ts, data_acc[-1]["timestamp"])
            if data_pos:
                last_ts = max(last_ts, data_pos[-1]["timestamp"])

            if last_ts == 0:
                break

            if len(data_acc) < limit and len(data_pos) < limit:
                break

            cursor = last_ts + 1
            # 额外等待
            time.sleep(1.5)

        except Exception as e:
            logger.error(f"多空比下载失败: {e}")
            break

    logger.info(
        f"多空比下载完成: 账户比 {len(all_data_ls_account)} 条, "
        f"持仓比 {len(all_data_ls_position)} 条"
    )
    return all_data_ls_account, all_data_ls_position


def fetch_ticker_snapshot(exchange, symbol, anti_ban):
    """获取当前 Ticker 快照 (24h统计)"""
    logger.info(f"获取 {symbol} Ticker 快照...")
    try:
        ticker = anti_ban.retry_with_backoff(exchange.fetch_ticker, symbol)
        return ticker
    except Exception as e:
        logger.error(f"Ticker 获取失败: {e}")
        return None


def fetch_order_book_snapshot(exchange, symbol, anti_ban, limit=500):
    """获取当前订单簿快照"""
    logger.info(f"获取 {symbol} 订单簿快照 (depth={limit})...")
    try:
        ob = anti_ban.retry_with_backoff(exchange.fetch_order_book, symbol, limit=limit)
        return ob
    except Exception as e:
        logger.error(f"订单簿获取失败: {e}")
        return None


def fetch_recent_trades(exchange, symbol, anti_ban, limit=1000):
    """获取最近成交记录"""
    logger.info(f"获取 {symbol} 最近成交 (limit={limit})...")
    try:
        trades = anti_ban.retry_with_backoff(exchange.fetch_trades, symbol, limit=limit)
        return trades
    except Exception as e:
        logger.error(f"最近成交获取失败: {e}")
        return None


# ============================================================
# 数据保存函数
# ============================================================

def ohlcv_to_dataframe(data, timeframe):
    """将 OHLCV 列表转为 DataFrame"""
    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.drop(columns=["timestamp"])
    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    return df


def save_dataframe(df, filepath, comment=""):
    """保存 DataFrame 为 CSV"""
    df.to_csv(filepath, index=False, encoding="utf-8")
    size_mb = filepath.stat().st_size / (1024 * 1024)
    logger.info(f"已保存: {filepath.name} ({len(df)} 行, {size_mb:.2f} MB) {comment}")


def save_json(data, filepath):
    """保存数据为 JSON"""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    size_mb = filepath.stat().st_size / (1024 * 1024)
    logger.info(f"已保存: {filepath.name} ({size_mb:.2f} MB)")


def save_dict_as_json(data, filepath):
    """保存字典数据为 JSON (如 ticker, orderbook)"""
    serializable = {}
    for k, v in data.items():
        try:
            json.dumps(v)
            serializable[k] = v
        except (TypeError, OverflowError):
            serializable[k] = str(v)
    save_json(serializable, filepath)


# ============================================================
# 主流程
# ============================================================

def main():
    """主函数"""
    # 创建数据目录
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 计算时间范围
    now = datetime.now(timezone.utc)
    since_dt = now - timedelta(days=DAYS)
    since_ms = int(since_dt.timestamp() * 1000)
    until_ms = int(now.timestamp() * 1000)

    # 初始化交易所和防封管理器
    exchange = create_exchange()
    anti_ban = BinanceAntiBan(exchange)

    # 检测市场类型 (本地=永续合约, GitHub Actions=现货)
    market_type = exchange.options.get("defaultType", "spot")
    is_future = market_type == "future"
    market_type_label = "永续合约" if is_future else "现货 (GitHub Actions 兼容模式)"

    logger.info("=" * 60)
    logger.info(f"Binance SOLUSDT 数据下载器")
    logger.info(f"交易对: {SYMBOL} ({market_type_label})")
    logger.info(f"K线周期: {', '.join(TIMEFRAMES)}")
    logger.info(f"时间范围: {since_dt.strftime('%Y-%m-%d')} ~ {now.strftime('%Y-%m-%d')} ({DAYS}天)")
    logger.info(f"请求间隔: {RATE_LIMIT_MS}ms | 最大重试: {MAX_RETRIES}次")
    logger.info(f"数据目录: {DATA_DIR.absolute()}")
    logger.info("=" * 60)

    start_total = time.time()

    # ----------------------------------------------------------
    # 1. 下载所有时间周期的 K 线数据
    # ----------------------------------------------------------
    ohlcv_dir = DATA_DIR / "ohlcv"
    ohlcv_dir.mkdir(parents=True, exist_ok=True)

    for tf in TIMEFRAMES:
        try:
            data = fetch_all_ohlcv(exchange, SYMBOL, tf, since_ms, until_ms, anti_ban)
            if data:
                df = ohlcv_to_dataframe(data, tf)
                save_dataframe(df, ohlcv_dir / f"SOLUSDT_{tf}.csv")
            else:
                logger.warning(f"{tf}: 无数据")
        except Exception as e:
            logger.error(f"{tf} K线下载异常: {e}")

    # ----------------------------------------------------------
    # 2. 下载合约特有衍生数据 (仅永续合约模式)
    # ----------------------------------------------------------
    derived_dir = DATA_DIR / "derived"
    derived_dir.mkdir(parents=True, exist_ok=True)

    if is_future:
        # 2a. 资金费率历史
        funding_data = fetch_funding_rate_history(exchange, SYMBOL, since_ms, until_ms, anti_ban)
        if funding_data:
            save_json(funding_data, derived_dir / "funding_rate_history.json")
            df_funding = pd.DataFrame(funding_data)
            if "timestamp" in df_funding.columns:
                df_funding["datetime"] = pd.to_datetime(df_funding["timestamp"], unit="ms", utc=True)
            save_dataframe(df_funding, derived_dir / "funding_rate_history.csv")

        # 2b. 未平仓合约历史 (使用1h周期)
        oi_timeframes = ["1h", "2h"]
        for tf in oi_timeframes:
            oi_data = fetch_open_interest_history(exchange, SYMBOL, tf, since_ms, until_ms, anti_ban)
            if oi_data:
                save_json(oi_data, derived_dir / f"open_interest_history_{tf}.json")
                df_oi = pd.DataFrame(oi_data)
                if "timestamp" in df_oi.columns:
                    df_oi["datetime"] = pd.to_datetime(df_oi["timestamp"], unit="ms", utc=True)
                save_dataframe(df_oi, derived_dir / f"open_interest_history_{tf}.csv")

        # 2c. 多空比历史 (使用1h和4h周期)
        ls_timeframes = ["1h", "4h"]
        for tf in ls_timeframes:
            acc_data, pos_data = fetch_long_short_ratio_history(
                exchange, SYMBOL, tf, since_ms, until_ms, anti_ban
            )
            if acc_data:
                save_json(acc_data, derived_dir / f"long_short_account_{tf}.json")
                df_acc = pd.DataFrame(acc_data)
                if "timestamp" in df_acc.columns:
                    df_acc["datetime"] = pd.to_datetime(df_acc["timestamp"], unit="ms", utc=True)
                save_dataframe(df_acc, derived_dir / f"long_short_account_{tf}.csv")
            if pos_data:
                save_json(pos_data, derived_dir / f"long_short_position_{tf}.json")
                df_pos = pd.DataFrame(pos_data)
                if "timestamp" in df_pos.columns:
                    df_pos["datetime"] = pd.to_datetime(df_pos["timestamp"], unit="ms", utc=True)
                save_dataframe(df_pos, derived_dir / f"long_short_position_{tf}.csv")
    else:
        logger.info(
            "当前使用现货模式 (data-api.binance.vision)，"
            "跳过合约特有数据: 资金费率 / 未平仓合约 / 多空比"
        )

    # ----------------------------------------------------------
    # 5. 下载 Ticker 快照 (24h 统计)
    # ----------------------------------------------------------
    ticker = fetch_ticker_snapshot(exchange, SYMBOL, anti_ban)
    if ticker:
        save_dict_as_json(ticker, derived_dir / "ticker_snapshot.json")

    # ----------------------------------------------------------
    # 6. 下载订单簿快照
    # ----------------------------------------------------------
    orderbook = fetch_order_book_snapshot(exchange, SYMBOL, anti_ban, limit=500)
    if orderbook:
        ob_dict = {
            "bids": orderbook.get("bids", []),
            "asks": orderbook.get("asks", []),
            "timestamp": orderbook.get("timestamp"),
            "datetime": orderbook.get("datetime"),
            "nonce": orderbook.get("nonce"),
        }
        save_json(ob_dict, derived_dir / "orderbook_snapshot.json")

    # ----------------------------------------------------------
    # 7. 下载最近成交记录
    # ----------------------------------------------------------
    trades = fetch_recent_trades(exchange, SYMBOL, anti_ban, limit=1000)
    if trades:
        trades_list = [t for t in trades]
        save_json(trades_list, derived_dir / "recent_trades.json")
        df_trades = pd.DataFrame(trades_list)
        if "timestamp" in df_trades.columns:
            df_trades["datetime"] = pd.to_datetime(df_trades["timestamp"], unit="ms", utc=True)
        save_dataframe(df_trades, derived_dir / "recent_trades.csv")

    # ----------------------------------------------------------
    # 完成统计
    # ----------------------------------------------------------
    elapsed_total = time.time() - start_total
    stats = anti_ban.stats()
    logger.info("=" * 60)
    logger.info("下载完成!")
    logger.info(f"总耗时: {elapsed_total:.1f} 秒 ({elapsed_total/60:.1f} 分钟)")
    logger.info(f"总请求数: {stats['total_requests']}")
    logger.info(f"平均间隔: {stats['avg_interval_ms']}ms")
    logger.info(f"数据保存目录: {DATA_DIR.absolute()}")

    # 列出所有已保存的文件
    logger.info("-" * 60)
    logger.info("已保存的文件:")
    for f in sorted(DATA_DIR.rglob("*")):
        if f.is_file():
            size = f.stat().st_size / (1024 * 1024)
            logger.info(f"  {f.relative_to(DATA_DIR)} ({size:.2f} MB)")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
