import requests
import pandas as pd
from datetime import datetime, timezone
import time
import numpy as np

# ===== 텔레그램 설정 =====
TELEGRAM_TOKEN = "7877227554:AAFvgL7_-2ajrcEEPAcZh_1BRqyusXtTwXc"
TELEGRAM_CHAT_ID = "6744830265"  # 여기에 실제 사용자 ID 입력
FUNDING_THRESHOLD = 0.013  # ±1.5%
CHECK_INTERVAL_MIN = 5     # 몇 분마다 반복 실행할지 (ex. 5분)
ORDERBOOK_DEPTH = 20       # Orderbook 깊이 (20개 레벨)
SLIPPAGE_TARGET = 0.1      # 슬리피지 계산 목표 비율 (10%)

# 알림 중복 방지를 위한 캐시
alerted_symbols = set()

# ===== Binance =====
def get_binance_predicted_funding_rates_via_ws(timeout=5):
    """
    Binance 모든 심볼의 예정 펀딩비(predictedFundingRate)와 nextFundingTime을 WebSocket으로 한 번에 수집
    최초 메시지 수신 후 DataFrame 반환 (timeout: 연결 대기 최대 초)
    """
    from datetime import datetime, timezone
    import pandas as pd
    funding_data = []
    received = {'done': False}

    def on_message(ws, message):
        data = json.loads(message)
        for entry in data:
            try:
                funding_data.append({
                    "exchange": "Binance",
                    "symbol": entry["s"],
                    "fundingRate": float(entry["r"]),
                    "nextFundingTime": pd.to_datetime(entry["T"], unit="ms").replace(tzinfo=timezone.utc)
                })
            except Exception:
                continue
        received['done'] = True
        ws.close()

    def on_error(ws, error):
        print(f"WebSocket 오류: {error}")
        received['done'] = True
        ws.close()

    ws = websocket.WebSocketApp(
        "wss://fstream.binance.com/ws/!markPrice@arr",
        on_message=on_message,
        on_error=on_error
    )
    import threading
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()
    import time
    t0 = time.time()
    while not received['done'] and time.time() - t0 < timeout:
        time.sleep(0.1)
    if not funding_data:
        print("WebSocket로 바이낸스 펀딩비 데이터 수신 실패")
        return pd.DataFrame()
    return pd.DataFrame(funding_data)

# 기존 REST 방식은 비효율적이므로 주석 처리 또는 삭제
# def get_binance_predicted_funding_rates(max_symbols=None):
#     ...

# ===== Bybit =====
import websocket
import json

import traceback

def get_bybit_linear_symbols():
    url = "https://api.bybit.com/v5/market/instruments"
    params = {"category": "linear"}
    resp = requests.get(url, params=params)
    data = resp.json()
    symbols = [s['symbol'] for s in data['result']['list']]
    return symbols


def get_bybit_latest_funding_rates():
    """
    Bybit 모든 linear perpetual 심볼의 최신 펀딩비(fundingRate)와 nextFundingTime을 /v5/market/tickers에서 한 번에 수집
    DataFrame 반환
    """
    import pandas as pd
    from datetime import datetime, timezone
    import requests

    url = "https://api.bybit.com/v5/market/tickers"
    params = {"category": "linear"}
    resp = requests.get(url, params=params)
    print("[Bybit API 응답 상태코드]", resp.status_code)
    print("[Bybit API 응답 원문 일부]", resp.text[:1000])
    if resp.status_code != 200:
        print(f"[Bybit tickers 오류] status={resp.status_code}, text={resp.text[:500]}")
        return pd.DataFrame(columns=["exchange", "symbol", "fundingRate", "nextFundingTime"])
    try:
        data = resp.json()
        symbols = data["result"]["list"]
        print(f"[Bybit 심볼 개수]: {len(symbols)}")
        print("[Bybit 첫 3개 심볼]", symbols[:3])
        for i, s in enumerate(symbols[:3]):
            print(f"[Bybit 샘플 심볼 {i}] symbol={s.get('symbol')}, fundingRate={s.get('fundingRate')}, nextFundingTime={s.get('nextFundingTime')}")
    except Exception as e:
        print(f"[Bybit tickers 파싱 오류] {e}")
        return pd.DataFrame(columns=["exchange", "symbol", "fundingRate", "nextFundingTime"])
    funding_data = []
    for s in symbols:
        if (
            s.get('symbol') and s.get('fundingRate') is not None
            and s.get('nextFundingTime') is not None
        ):
            try:
                funding_data.append({
                    "exchange": "Bybit",
                    "symbol": s["symbol"],
                    "fundingRate": float(s["fundingRate"]),
                    "nextFundingTime": pd.to_datetime(int(s["nextFundingTime"]), unit="ms").replace(tzinfo=timezone.utc)
                })
            except Exception as e:
                print(f"[Bybit 펀딩비 파싱 오류] {s.get('symbol')}: {e}")
    print(f"[Bybit 최종 funding_data 개수]: {len(funding_data)}")
    if funding_data:
        print("[Bybit 샘플 funding_data]", funding_data[:3])
        return pd.DataFrame(funding_data)
    else:
        return pd.DataFrame(columns=["exchange", "symbol", "fundingRate", "nextFundingTime"])



# ===== Orderbook 분석 =====
def get_binance_orderbook(symbol):
    # 심볼 형식 확인 (USDT가 없으면 추가)
    if not symbol.endswith('USDT'):
        symbol = f"{symbol}USDT"
    
    # 심볼을 대문자로 변환
    symbol = symbol.upper()
    
    # 바이낸스 선물 API의 올바른 Orderbook 엔드포인트 사용
    url = f"https://fapi.binance.com/fapi/v1/depth"
    params = {
        "symbol": symbol,
        "limit": ORDERBOOK_DEPTH
    }
    try:
        print(f"바이낸스 Orderbook 요청:")
        print(f"- 심볼: {symbol}")
        print(f"- URL: {url}")
        print(f"- 파라미터: {params}")
        
        res = requests.get(url, params=params)
        print(f"바이낸스 Orderbook 응답 상태: {res.status_code}")
        print(f"바이낸스 Orderbook 응답 헤더: {res.headers}")
        
        if res.status_code != 200:
            print(f"바이낸스 Orderbook 오류 응답: {res.text[:500]}")
            return None, None
        
        data = res.json()
        print(f"바이낸스 Orderbook 응답 데이터: {data}")
        
        if 'bids' not in data or 'asks' not in data:
            print(f"바이낸스 Orderbook 응답에 bids/asks 키가 없습니다: {data}")
            return None, None
            
        if not data['bids'] or not data['asks']:
            print(f"바이낸스 Orderbook이 비어있습니다: {data}")
            # 심볼이 거래 가능한지 확인
            ticker_url = f"https://fapi.binance.com/fapi/v1/ticker/24hr"
            ticker_params = {"symbol": symbol}
            ticker_res = requests.get(ticker_url, params=ticker_params)
            if ticker_res.status_code == 200:
                ticker_data = ticker_res.json()
                print(f"바이낸스 티커 데이터: {ticker_data}")
                if 'volume' in ticker_data and float(ticker_data['volume']) == 0:
                    print(f"심볼 {symbol}은 거래량이 0입니다.")
            return None, None
            
        bids = pd.DataFrame(data['bids'], columns=['price', 'quantity'], dtype=float)
        asks = pd.DataFrame(data['asks'], columns=['price', 'quantity'], dtype=float)
        
        print(f"바이낸스 Orderbook 데이터:")
        print(f"- 매수 호가 수: {len(bids)}")
        print(f"- 매도 호가 수: {len(asks)}")
        if len(bids) > 0 and len(asks) > 0:
            print(f"- 최우선 매수호가: {bids.iloc[0]['price']} USDT")
            print(f"- 최우선 매도호가: {asks.iloc[0]['price']} USDT")
        
        return bids, asks
    except Exception as e:
        print(f"바이낸스 Orderbook 오류 ({symbol}): {e}")
        return None, None

def get_bybit_orderbook(symbol):
    url = "https://api.bybit.com/v5/market/orderbook"
    params = {
        "category": "linear",
        "symbol": symbol,
        "limit": ORDERBOOK_DEPTH
    }
    try:
        print(f"바이비트 Orderbook 요청: {symbol}")
        res = requests.get(url, params=params)
        print(f"바이비트 Orderbook 응답 상태: {res.status_code}")
        
        if res.status_code != 200:
            print(f"바이비트 Orderbook 오류 응답: {res.text[:500]}")
            return None, None
        
        data = res.json()
        if "result" not in data:
            print(f"바이비트 Orderbook 응답에 result 키가 없습니다: {data}")
            return None, None
            
        result = data["result"]
        bids = pd.DataFrame(result['b'], columns=['price', 'quantity'], dtype=float)
        asks = pd.DataFrame(result['a'], columns=['price', 'quantity'], dtype=float)
        
        print(f"바이비트 Orderbook 데이터:")
        print(f"- 매수 호가 수: {len(bids)}")
        print(f"- 매도 호가 수: {len(asks)}")
        if len(bids) > 0 and len(asks) > 0:
            print(f"- 최우선 매수호가: {bids.iloc[0]['price']} USDT")
            print(f"- 최우선 매도호가: {asks.iloc[0]['price']} USDT")
        
        return bids, asks
    except Exception as e:
        print(f"바이비트 Orderbook 오류 ({symbol}): {e}")
        return None, None

def calculate_available_volume(bids, asks, current_price, symbol):
    """현재 가격 기준 ±1% 범위 내의 거래 가능한 볼륨을 계산"""
    if bids is None or asks is None:
        print("Orderbook 데이터가 없습니다.")
        return None, None, None
    
    if len(bids) == 0 or len(asks) == 0:
        print("Orderbook이 비어있습니다.")
        return None, None, None
    
    try:
        # ±1% 가격 범위 계산
        price_range_min = current_price * 0.99  # -1%
        price_range_max = current_price * 1.01  # +1%
        
        print(f"가격 범위 분석:")
        print(f"- 현재 가격: {current_price} USDT")
        print(f"- 최소 가격: {price_range_min} USDT (-1%)")
        print(f"- 최대 가격: {price_range_max} USDT (+1%)")
        
        # 매수 가능 볼륨 계산 (매도 호가 중 ±1% 범위 내)
        buy_volume = 0
        buy_value = 0
        for _, row in asks.iterrows():
            price = row['price']
            if price > price_range_max:
                break
            if price >= price_range_min:
                quantity = row['quantity']
                buy_volume += quantity
                buy_value += price * quantity
        
        # 매도 가능 볼륨 계산 (매수 호가 중 ±1% 범위 내)
        sell_volume = 0
        sell_value = 0
        for _, row in bids.iterrows():
            price = row['price']
            if price < price_range_min:
                break
            if price <= price_range_max:
                quantity = row['quantity']
                sell_volume += quantity
                sell_value += price * quantity
        
        # 평균 거래 가능 볼륨 계산
        avg_volume = (buy_volume + sell_volume) / 2
        avg_value = (buy_value + sell_value) / 2
        
        # 심볼에서 코인 심볼 추출 (예: BTCUSDT -> BTC)
        coin_symbol = symbol.replace('USDT', '')
        
        print(f"거래 가능 볼륨 분석:")
        print(f"- 매수 가능 볼륨: {buy_volume:.2f} {coin_symbol} ({buy_value:.2f} USDT)")
        print(f"- 매도 가능 볼륨: {sell_volume:.2f} {coin_symbol} ({sell_value:.2f} USDT)")
        print(f"- 평균 거래 가능 볼륨: {avg_volume:.2f} {coin_symbol} ({avg_value:.2f} USDT)")
        
        return avg_volume, avg_value, current_price
    except Exception as e:
        print(f"볼륨 계산 중 오류 발생: {e}")
        return None, None, None

# ===== 펀딩 메시지 포맷 =====
def format_funding_alert(df, is_normal=False):
    if is_normal:
        msg = "<b>✅ 펀딩비 정상</b>\n\n"
        msg += f"모든 펀딩비가 {FUNDING_THRESHOLD*100:.1f}% 이내입니다."
    else:
        msg = "<b>⚠️ 비정상 펀딩비 감지</b>\n\n"
        for _, row in df.iterrows():
            rate_pct = row["fundingRate"] * 100
            symbol = row["symbol"]
            exchange = row["exchange"]
            msg += f"📉 <b>{exchange}</b> | <code>{symbol}</code> | {rate_pct:.2f}%"

            # 펀딩비 부과 시각과 남은 시간 계산 (UTC 기준)
            if pd.notnull(row["nextFundingTime"]):
                now = datetime.now(timezone.utc)
                next_time = row["nextFundingTime"]
                time_diff = next_time - now

                # 남은 시간을 시:분:초 형식으로 변환 (UTC)
                total_seconds = int(time_diff.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60

                msg += f"\n   ⏰ 다음 펀딩비 부과: {next_time.strftime('%H:%M:%S')} (UTC)"
                msg += f"\n   ⏳ 남은 시간: {hours:02d}:{minutes:02d}:{seconds:02d}"

            # Orderbook 분석 추가
            if exchange == "Binance":
                bids, asks = get_binance_orderbook(symbol)
            else:
                bids, asks = get_bybit_orderbook(symbol)

            if bids is not None and asks is not None:
                current_price = (bids.iloc[0]['price'] + asks.iloc[0]['price']) / 2
                avg_volume, avg_value, _ = calculate_available_volume(bids, asks, current_price, symbol)
                if avg_volume is not None:
                    coin_symbol = symbol.replace('USDT', '')
                    msg += f"\n   📊 유동성 분석 (±1% 가격 범위):"
                    msg += f"\n   - 현재 가격: {current_price} USDT"
                    msg += f"\n   - 평균 거래 가능 볼륨: {avg_volume:.2f} {coin_symbol}"
                    msg += f"\n   - 평균 거래 가능 금액: {avg_value:.2f} USDT"
            msg += "\n"
    return msg

# ===== 텔레그램 메시지 전송 =====
def send_telegram_message(token, chat_id, message):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        print(f"텔레그램 API 호출 시도: {url}")
        print(f"전송할 메시지: {message}")
        response = requests.post(url, data=payload)
        print(f"텔레그램 API 응답 상태 코드: {response.status_code}")
        print(f"텔레그램 API 응답 내용: {response.text}")
        
        if response.status_code != 200:
            print(f"텔레그램 API 오류 응답: {response.text}")
            return False
            
        return True
    except requests.exceptions.RequestException as e:
        print(f"텔레그램 API 요청 오류: {str(e)}")
        return False
    except Exception as e:
        print(f"텔레그램 알림 전송 중 예상치 못한 오류: {str(e)}")
        return False

# ===== 감시 실행 =====
def run_alert_bot():
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} (UTC)] 🔍 펀딩비 감시 중...")
    try:
        binance_df = get_binance_predicted_funding_rates_via_ws()
        bybit_df = get_bybit_latest_funding_rates()
    except Exception as e:
        print("데이터 수집 오류:", e)
        return

    # 빈 데이터프레임 처리
    if binance_df.empty and bybit_df.empty:
        print("모든 데이터프레임이 비어 있습니다. 거래소 API에서 데이터를 받지 못했습니다.")
        return
    
    # 결합 대신 각 데이터프레임을 개별적으로 처리
    all_results = []
    
    # Binance 진단
    print("[Binance 전체 펀딩비]", binance_df.head())
    print("[Binance 임계값 초과]", binance_df[binance_df["fundingRate"].abs() >= FUNDING_THRESHOLD].head())
    # 바이낸스 데이터 처리
    if not binance_df.empty:
        binance_extreme = binance_df[binance_df["fundingRate"].abs() >= FUNDING_THRESHOLD]
        binance_new_alerts = binance_extreme[~binance_extreme["symbol"].isin(alerted_symbols)]
        print("[Binance 신규 알림 대상]", binance_new_alerts.head())
        if not binance_new_alerts.empty:
            all_results.append(binance_new_alerts)

    # Bybit 진단
    print("[Bybit 전체 펀딩비 컬럼]", bybit_df.columns)
    print("[Bybit 전체 펀딩비]", bybit_df.head())
    if "fundingRate" in bybit_df.columns:
        print("[Bybit 임계값 초과]", bybit_df[bybit_df["fundingRate"].abs() >= FUNDING_THRESHOLD].head())
    else:
        print("[Bybit] 'fundingRate' 컬럼 없음! 실제 컬럼:", bybit_df.columns)

    # 바이비트 데이터 처리
    if not bybit_df.empty:
        bybit_extreme = bybit_df[bybit_df["fundingRate"].abs() >= FUNDING_THRESHOLD]
        bybit_new_alerts = bybit_extreme[~bybit_extreme["symbol"].isin(alerted_symbols)]
        print("[Bybit 신규 알림 대상]", bybit_new_alerts.head())
        if not bybit_new_alerts.empty:
            all_results.append(bybit_new_alerts)

    # 결과 데이터가 있는 경우에만 처리
    if all_results:
        # reset_index 사용하여 안전하게 결합 (concat 사용 안함)
        if len(all_results) == 1:
            new_alerts = all_results[0]
        else:
            # 수동으로 데이터프레임 생성하여 concat 사용 회피
            alert_rows = []
            for df in all_results:
                for idx, row in df.iterrows():
                    alert_rows.append({
                        "exchange": row["exchange"],
                        "symbol": row["symbol"],
                        "fundingRate": row["fundingRate"],
                        "nextFundingTime": row["nextFundingTime"]
                    })
            new_alerts = pd.DataFrame(alert_rows)
        
        # 알림 전송
        msg = format_funding_alert(new_alerts)
        success = send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, msg)
        if success:
            print("✅ 텔레그램 알림 전송")
            # 각 데이터프레임에서 알림 보낸 심볼 추가
            for df in all_results:
                alerted_symbols.update(df["symbol"].tolist())
        else:
            print("❌ 텔레그램 전송 실패")
    else:
        # 정상 상태 알림 전송
        msg = format_funding_alert(None, is_normal=True)
        success = send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, msg)
        if success:
            print("✅ 정상 상태 알림 전송")
        else:
            print("❌ 정상 상태 알림 전송 실패")

# ===== 루프 실행 =====
if __name__ == "__main__":
    run_alert_bot()
