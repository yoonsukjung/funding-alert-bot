import requests
import pandas as pd
from datetime import datetime, timezone
import time
import numpy as np

# ===== 텔레그램 설정 =====
TELEGRAM_TOKEN = "7877227554:AAFvgL7_-2ajrcEEPAcZh_1BRqyusXtTwXc"
# 사용자의 실제 채팅 ID로 변경 필요 (봇 ID가 아님)
# 채팅 ID를 얻는 방법:
# 1. @userinfobot 봇에게 메시지 보내기
# 2. @getidsbot 봇 사용
TELEGRAM_CHAT_ID = "6744830265"  # 여기에 실제 사용자 ID 입력
FUNDING_THRESHOLD = 0.013  # ±1.5%
CHECK_INTERVAL_MIN = 5     # 몇 분마다 반복 실행할지 (ex. 5분)
ORDERBOOK_DEPTH = 20       # Orderbook 깊이 (20개 레벨)
SLIPPAGE_TARGET = 0.1      # 슬리피지 계산 목표 비율 (10%)

# 알림 중복 방지를 위한 캐시
alerted_symbols = set()

# ===== Binance =====
def get_binance_funding_rates():
    # 최신 펀딩비 정보를 가져오는 엔드포인트 사용
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    try:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] 바이낸스 API 호출: {url}")
        res = requests.get(url)
        print(f"바이낸스 API 응답 상태 코드: {res.status_code}")
        
        if res.status_code != 200:
            print(f"바이낸스 API 오류 응답: {res.text[:500]}")
            return pd.DataFrame()
            
        data = res.json()
        print(f"바이낸스 API 응답 데이터 개수: {len(data)}")
        
        # 거래 가능한 심볼 목록 가져오기
        exchange_info_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        exchange_info_res = requests.get(exchange_info_url)
        if exchange_info_res.status_code == 200:
            exchange_info = exchange_info_res.json()
            active_symbols = {symbol['symbol'] for symbol in exchange_info['symbols'] if symbol['status'] == 'TRADING'}
            print(f"거래 가능한 심볼 수: {len(active_symbols)}")
        else:
            print(f"거래 가능한 심볼 목록 가져오기 실패: {exchange_info_res.text[:500]}")
            active_symbols = set()
        
        funding_data = []
        for entry in data:
            if entry.get("fundingRate") is not None:
                symbol = entry["symbol"]
                # 거래 가능한 심볼만 처리
                if symbol in active_symbols:
                    # 펀딩 시간을 UTC로 변환
                    funding_time = pd.to_datetime(entry["fundingTime"], unit="ms")
                    funding_time = funding_time.replace(tzinfo=timezone.utc)
                    
                    # 다음 펀딩 시간 계산 (8시간 간격)
                    next_funding = funding_time + pd.Timedelta(hours=8)
                    
                    funding_data.append({
                        "exchange": "Binance",
                        "symbol": symbol,
                        "fundingRate": float(entry["fundingRate"]),
                        "nextFundingTime": next_funding
                    })
                else:
                    print(f"비활성화된 심볼 건너뛰기: {symbol}")
        
        # 결과가 비어있는 경우 빈 데이터프레임 반환
        if not funding_data:
            return pd.DataFrame()
            
        return pd.DataFrame(funding_data)
    except requests.exceptions.RequestException as e:
        print(f"바이낸스 API 요청 오류: {e}")
        return pd.DataFrame()
    except ValueError as e:
        print(f"바이낸스 데이터 파싱 오류: {e}")
        if 'res' in locals():
            print(f"바이낸스 원본 응답: {res.text[:500]}")
        return pd.DataFrame()

# ===== Bybit =====
def get_bybit_funding_rates():
    # V5 API 사용
    url = "https://api.bybit.com/v5/market/tickers"
    params = {"category": "linear"}
    try:
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] 바이비트 API 호출: {url}")
        res = requests.get(url, params=params)
        print(f"바이비트 API 응답 상태 코드: {res.status_code}")
        
        if res.status_code != 200:
            print(f"바이비트 API 오류 응답: {res.text[:500]}")
            return pd.DataFrame()
            
        data = res.json()
        
        if "result" not in data or "list" not in data["result"]:
            print(f"바이비트 API 응답에 올바른 데이터 구조가 없습니다: {str(data)[:500]}")
            return pd.DataFrame()
            
        print(f"바이비트 API 응답 데이터 개수: {len(data['result']['list'])}")
        
        funding_data = []
        for entry in data["result"]["list"]:
            # 펀딩비 데이터가 있는 심볼만 처리
            if "fundingRate" in entry and entry["fundingRate"] and entry["fundingRate"] != "":
                try:
                    # 다음 펀딩 시간 계산 (8시간 간격)
                    now = datetime.now(timezone.utc)
                    # 현재 시간을 8시간 단위로 올림
                    next_funding = now + pd.Timedelta(hours=8 - (now.hour % 8))
                    # 초와 마이크로초를 0으로 설정
                    next_funding = next_funding.replace(minute=0, second=0, microsecond=0)
                    
                    funding_data.append({
                        "exchange": "Bybit",
                        "symbol": entry["symbol"],
                        "fundingRate": float(entry["fundingRate"]),
                        "nextFundingTime": next_funding
                    })
                except (ValueError, TypeError) as e:
                    print(f"바이비트 '{entry['symbol']}' 펀딩비 파싱 오류: {e}, 값: '{entry['fundingRate']}'")
                    continue
        return pd.DataFrame(funding_data)
    except requests.exceptions.RequestException as e:
        print(f"바이비트 API 요청 오류: {e}")
        return pd.DataFrame()
    except ValueError as e:
        print(f"바이비트 데이터 파싱 오류: {e}")
        if 'res' in locals():
            print(f"바이비트 원본 응답: {res.text[:500]}")
        return pd.DataFrame()

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
            
            # 펀딩비 부과 시각과 남은 시간 계산
            if pd.notnull(row["nextFundingTime"]):
                now = datetime.now(timezone.utc)
                next_time = row["nextFundingTime"]
                time_diff = next_time - now
                
                # UTC를 KST로 변환 (UTC+9)
                kst_time = next_time + pd.Timedelta(hours=9)
                
                # 남은 시간을 시:분:초 형식으로 변환
                hours = time_diff.seconds // 3600
                minutes = (time_diff.seconds % 3600) // 60
                seconds = time_diff.seconds % 60
                
                msg += f"\n   ⏰ 다음 펀딩비 부과: {kst_time.strftime('%H:%M:%S')} (KST)"
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
    print(f"[{(datetime.now(timezone.utc) + pd.Timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')} (KST)] 🔍 펀딩비 감시 중...")
    try:
        binance_df = get_binance_funding_rates()
        bybit_df = get_bybit_funding_rates()
    except Exception as e:
        print("데이터 수집 오류:", e)
        return

    # 빈 데이터프레임 처리
    if binance_df.empty and bybit_df.empty:
        print("모든 데이터프레임이 비어 있습니다. 거래소 API에서 데이터를 받지 못했습니다.")
        return
    
    # 결합 대신 각 데이터프레임을 개별적으로 처리
    all_results = []
    
    # 바이낸스 데이터 처리
    if not binance_df.empty:
        binance_extreme = binance_df[binance_df["fundingRate"].abs() >= FUNDING_THRESHOLD]
        binance_new_alerts = binance_extreme[~binance_extreme["symbol"].isin(alerted_symbols)]
        if not binance_new_alerts.empty:
            all_results.append(binance_new_alerts)
    
    # 바이비트 데이터 처리
    if not bybit_df.empty:
        bybit_extreme = bybit_df[bybit_df["fundingRate"].abs() >= FUNDING_THRESHOLD]
        bybit_new_alerts = bybit_extreme[~bybit_extreme["symbol"].isin(alerted_symbols)]
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
    while True:
        now = datetime.now(timezone.utc)
        # 현재 시간이 매시 55분인 경우에만 체크
        if now.minute == 55:
            print(f"[{(now + pd.Timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')} (KST)] 🔍 펀딩비 감시 중...")
            run_alert_bot()
            # 다음 체크를 위해 1분 대기
            time.sleep(60)
        else:
            # 다음 체크 시간까지 대기 (1분)
            time.sleep(60)
