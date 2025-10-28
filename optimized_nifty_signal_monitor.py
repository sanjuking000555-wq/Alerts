from SmartApi import SmartConnect
import pyotp
import logging
import time
from datetime import datetime, timedelta
import pytz
import requests
import json
from collections import deque
from functools import lru_cache

# Add near the top after imports
VERBOSE_MODE = False  # Set to False for production

# === USER CONFIG ===
API_KEY = "pCoAFqis"
CLIENT_CODE = "v478997"
PASSWORD = "5555"
TOTP_SECRET = "57JNUTFBWT4UOU5OARPNVM2UNM"

TELEGRAM_TOKEN = "8227438755:AAG1yOsHn6ysCGcoI1JcToChFC7niMinmbU"
TELEGRAM_CHAT_ID = "5837597618"


# Instruments - Fixed NIFTY token and symbol
INSTRUMENTS = {
    "NIFTY": {
        "token": "99926000",
        "exchange": "NSE",
        "tradingsymbol": "NIFTY 50"
    },
    "BANKNIFTY": {
        "token": "99926009",
        "exchange": "NSE",
        "tradingsymbol": "Nifty Bank"
    }
}

# Global variables
smart_api = None
last_alerts = {}
processed_candles = deque(maxlen=1000)  # Optimized: Use deque with max length to prevent memory bloat

# Pre-compute timezone for performance
IST = pytz.timezone("Asia/Kolkata")

# Interval mapping as constant
INTERVAL_MAP = {
    "3m": "THREE_MINUTE", 
    "5m": "FIVE_MINUTE"
}

# Logging - Optimized configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nifty_signals.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Set console encoding to UTF-8 for Windows
import sys
if sys.platform.startswith('win'):
    import os
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Optimized: Use session for connection pooling
telegram_session = requests.Session()

def send_telegram(msg):
    """Send message to Telegram - Optimized with session reuse"""
    try:
        # Escape unsafe characters for HTML
        safe_msg = msg.replace("<", "&lt;").replace(">", "&gt;")

        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": safe_msg,
            "parse_mode": "HTML"
        }
        response = telegram_session.post(url, data=payload, timeout=10)
        if response.status_code == 200:
            logging.info("Telegram message sent")
        else:
            logging.error(f"Telegram error: {response.text}")
    except Exception as e:
        logging.error(f"Telegram error: {e}")


def get_exact_candle_data(symbol, timeframe, candle_count=5):
    """Get EXACT candle data from Angel One API - Optimized"""
    try:
        if not smart_api:
            return []
            
        instrument = INSTRUMENTS[symbol]
        interval = INTERVAL_MAP.get(timeframe, "THREE_MINUTE")
        
        # Get current time in IST (use pre-computed IST)
        now = datetime.now(IST)
        
        # Get data from a few hours back
        from_date = now - timedelta(hours=3)
        
        params = {
            "exchange": instrument["exchange"],
            "symboltoken": instrument["token"],
            "interval": interval,
            "fromdate": from_date.strftime("%Y-%m-%d %H:%M"),
            "todate": now.strftime("%Y-%m-%d %H:%M")
        }
        
        response = smart_api.getCandleData(params)
        
        if response.get("status") and response.get("data"):
            raw_data = response["data"]
            
            # Optimized: Pre-allocate list and use list comprehension where possible
            candles = []
            for candle in raw_data[-candle_count:]:
                try:
                    timestamp = datetime.strptime(candle[0], "%Y-%m-%dT%H:%M:%S%z")
                    candles.append({
                        "time": timestamp.astimezone(IST),
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": int(candle[5]) if len(candle) > 5 else 0
                    })
                except Exception as e:
                    logging.error(f"Error parsing candle: {e}")
                    continue
            
            return candles
        else:
            logging.error(f"API Error for {symbol}: {response}")
            return []
            
    except Exception as e:
        logging.error(f"Error fetching {symbol} {timeframe}: {e}")
        return []


def check_signal_conditions(symbol, timeframe, candles):
    """Check for EXACT bullish/bearish signals - Optimized"""
    try:
        if len(candles) < 3:
            return None
            
        # Get the last two completed candles
        prev_candle = candles[-3]
        latest_candle = candles[-2]
        
        # Extract OHLC values
        prev_open = prev_candle["open"]
        prev_close = prev_candle["close"] 
        latest_open = latest_candle["open"]
        latest_close = latest_candle["close"]
        
        # Optimized: Create unique identifier once
        candle_id = f"{symbol}_{timeframe}_{latest_candle['time'].strftime('%Y%m%d_%H%M')}"
        if candle_id in processed_candles:
            return None
        
        # Optimized: Calculate all conditions at once
        bullish_cond1 = latest_open > prev_close      # Gap up
        bullish_cond2 = prev_open > prev_close        # Previous candle GREEN (fixed logic)
        bullish_cond3 = latest_close > latest_open    # Current candle GREEN
        
        bearish_cond1 = latest_open < prev_close      # Gap down
        bearish_cond2 = prev_open < prev_close        # Previous candle RED
        bearish_cond3 = latest_close < latest_open    # Current candle RED
        
        # Calculate gap percentage once
        gap_percent = ((latest_open - prev_close) / prev_close) * 100
        
        # Optimized: Only log detailed info if debugging
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"{'='*60}")
            logging.debug(f"CHECKING {symbol} {timeframe.upper()} at {latest_candle['time'].strftime('%d-%b %H:%M')}")
            logging.debug(f"   Gap %: {gap_percent:.3f}%")
            logging.debug(f"BULLISH CHECK: Gap:{bullish_cond1} PrevGreen:{bullish_cond2} CurrGreen:{bullish_cond3}")
            logging.debug(f"BEARISH CHECK: Gap:{bearish_cond1} PrevRed:{bearish_cond2} CurrRed:{bearish_cond3}")
            logging.debug(f"{'='*60}")
        
        # Optimized: Use all() for condition checking
        bullish_signal = all([bullish_cond1, bullish_cond2, bullish_cond3])
        bearish_signal = all([bearish_cond1, bearish_cond2, bearish_cond3])
        
        # Generate alerts
        if bullish_signal:
            processed_candles.append(candle_id)
            # Optimized: Use f-string with minimal formatting
            prev_h, prev_l = prev_candle['high'], prev_candle['low']
            latest_h, latest_l = latest_candle['high'], latest_candle['low']
            gap_pts = latest_open - prev_close
            
            return (
                f"🚀 <b>BULLISH SIGNAL CONFIRMED!</b>\n\n"
                f"📊 <b>{symbol} {timeframe.upper()}</b>\n"
                f"🕐 Time: {latest_candle['time'].strftime('%d-%b %H:%M')}\n\n"
                f"📈 <b>CONDITIONS MET:</b>\n"
                f"✅ Gap Up: {latest_open:.2f} > {prev_close:.2f}\n"
                f"✅ Prev Green: {prev_open:.2f} < {prev_close:.2f}\n"
                f"✅ Curr Green: {latest_close:.2f} > {latest_open:.2f}\n\n"
                f"📊 <b>CANDLE DATA:</b>\n"
                f"Previous: O:{prev_open:.2f} H:{prev_h:.2f} L:{prev_l:.2f} C:{prev_close:.2f}\n"
                f"Current:  O:{latest_open:.2f} H:{latest_h:.2f} L:{latest_l:.2f} C:{latest_close:.2f}\n\n"
                f"🎯 Gap: {gap_pts:+.2f} pts ({gap_percent:+.2f}%)"
            )
                   
        elif bearish_signal:
            processed_candles.append(candle_id)
            prev_h, prev_l = prev_candle['high'], prev_candle['low']
            latest_h, latest_l = latest_candle['high'], latest_candle['low']
            gap_pts = latest_open - prev_close
            
            return (
                f"📉 <b>BEARISH SIGNAL CONFIRMED!</b>\n\n"
                f"📊 <b>{symbol} {timeframe.upper()}</b>\n"
                f"🕐 Time: {latest_candle['time'].strftime('%d-%b %H:%M')}\n\n"
                f"📉 <b>CONDITIONS MET:</b>\n"
                f"✅ Gap Down: {latest_open:.2f} < {prev_close:.2f}\n"
                f"✅ Prev Red: {prev_open:.2f} > {prev_close:.2f}\n"
                f"✅ Curr Red: {latest_close:.2f} < {latest_open:.2f}\n\n"
                f"📊 <b>CANDLE DATA:</b>\n"
                f"Previous: O:{prev_open:.2f} H:{prev_h:.2f} L:{prev_l:.2f} C:{prev_close:.2f}\n"
                f"Current:  O:{latest_open:.2f} H:{latest_h:.2f} L:{latest_l:.2f} C:{latest_close:.2f}\n\n"
                f"🎯 Gap: {gap_pts:+.2f} pts ({gap_percent:+.2f}%)"
            )
        
        else:
            # Optimized: Only log if at INFO level
            if logging.getLogger().isEnabledFor(logging.INFO):
                bullish_count = sum([bullish_cond1, bullish_cond2, bullish_cond3])
                bearish_count = sum([bearish_cond1, bearish_cond2, bearish_cond3])
                logging.info(f"No Signal - Bullish: {bullish_count}/3, Bearish: {bearish_count}/3")
            return None
            
    except Exception as e:
        logging.error(f"Error in signal check: {e}")
        return None


def smart_timing_monitor():
    """Smart monitoring with precise timing - Optimized"""
    logging.info("Starting SMART TIMING monitor...")
    
    # Pre-compute market hours
    last_minute_checked = -1  # Track to avoid redundant checks

    while True:
        try:
            now = datetime.now(IST)
            current_time_str = now.strftime('%H:%M:%S')

            # Market hours check (optimized to check only once per minute)
            current_minute = now.minute
            if current_minute != last_minute_checked:
                market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
                market_end = now.replace(hour=15, minute=30, second=0, microsecond=0)

                if not (market_start <= now <= market_end):
                    logging.info(f"Outside market hours. Current: {current_time_str}")
                    time.sleep(60)
                    continue
                
                last_minute_checked = current_minute

            current_second = now.second

            # Optimized: Check 5m intervals (3m commented as per original)
            if current_minute % 5 == 0 and 1 <= current_second <= 2:
                logging.info(f"\nOPTIMAL CHECK TIME: {current_time_str} (5m)")
                
                # Optimized: Process all symbols in one go
                for symbol in INSTRUMENTS:
                    try:
                        candles = get_exact_candle_data(symbol, "5m", candle_count=3)
                        if len(candles) >= 3:
                            alert = check_signal_conditions(symbol, "5m", candles)
                            if alert:
                                logging.info(f"5M SIGNAL DETECTED for {symbol}!")
                                send_telegram(alert)
                    except Exception as e:
                        logging.error(f"Error checking {symbol} 5m: {e}")

            # Optimized: Sleep strategically
            # If we just checked, sleep for 3 seconds to avoid re-checking
            if current_minute % 5 == 0 and 1 <= current_second <= 2:
                time.sleep(3)
            else:
                time.sleep(1)

        except KeyboardInterrupt:
            logging.info("Stopping smart timing monitor...")
            break
        except Exception as e:
            logging.error(f"Error in smart timing loop: {e}")
            time.sleep(5)


def run():
    """Main function - Optimized"""
    global smart_api
    
    try:
        # Step 1: Login to Angel One
        logging.info("Connecting to Angel One...")
        otp = pyotp.TOTP(TOTP_SECRET).now()
        logging.info(f"Generated OTP: {otp}")
        
        smart_api = SmartConnect(api_key=API_KEY)
        data = smart_api.generateSession(CLIENT_CODE, PASSWORD, otp)
        
        if not data.get("status"):
            logging.error(f"Login failed: {data}")
            return
            
        logging.info("Successfully connected to Angel One")
        logging.info(f"Auth Token: {data['data']['jwtToken'][:20]}...")
        
        # Step 2: Send startup notification
        send_telegram(
            "NIFTY SIGNAL MONITOR STARTED\n\n"
            "Monitoring: NIFTY & BANKNIFTY\n"
            "Timeframes: 5min\n"
            "Using EXACT Angel One OHLC data\n\n"
            "Bullish: Gap up + Prev green + Curr green\n"
            "Bearish: Gap down + Prev red + Curr red\n\n"
            "System Ready!"
        )
        
        # Step 3: Start smart timing monitoring
        smart_timing_monitor()
        
    except Exception as e:
        logging.error(f"Main error: {e}")
        send_telegram(f"System Error: {e}")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logging.info("System stopped by user")
        send_telegram("Nifty Signal Monitor Stopped")
    except Exception as e:
        logging.error(f"Critical error: {e}")
        send_telegram(f"Critical Error: {e}")
