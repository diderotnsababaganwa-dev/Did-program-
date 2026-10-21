import asyncio
import json
import os
import time
import websockets
import eth_account
from hyperliquid.utils import constants
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

# =====================================================================
# ⚙️ ENVIRONMENT SWITCHES (Controlled safely via your Render dashboard)
# =====================================================================
IS_MAINNET = os.environ.get("IS_MAINNET", "False").lower() in ("true", "1", "yes")
AGENT_KEY = os.environ.get("HL_LIVE_AGENT_KEY") if IS_MAINNET else os.environ.get("HL_TESTNET_AGENT_KEY")

# --- EXACT USER PARAMETERS ---
RISK_PERCENT = 0.01         # Strict 1% risk of total capital per trade
SL_PERCENT = 0.01           # Stop Loss set at 1% distance
TP_PERCENT = 0.03           # Take Profit set at 3% distance (Exact 1:3 Ratio)
COOLDOWN_TIME = 60          # 60-second anti-plugin ban pause per coin

# =====================================================================
# 🌐 DYNAMIC NETWORK ROUTING MATRIX
# =====================================================================
if IS_MAINNET:
    API_URL = constants.MAINNET_API_URL
    WS_URL = "wss://api.hyperliquid.xyz/ws"
else:
    API_URL = constants.TESTNET_API_URL
    WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"

COMMON_CRYPTO_UNIVERSE = ["BTC", "ETH", "SOL", "AVAX", "SUI", "XRP", "LINK", "ADA", "DOT", "NEAR"]
prices = {coin: {"binance": 0.0, "hl": 0.0, "cooldown_until": 0.0} for coin in COMMON_CRYPTO_UNIVERSE}

async def stream_binance():
    streams = "/".join([f"{coin.lower()}usdt@ticker" for coin in COMMON_CRYPTO_UNIVERSE])
    uri = f"wss://://binance.com{streams}"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                while True:
                    message = await websocket.recv()
                    event = json.loads(message)
                    stream_name = event.get("stream", "")
                    data = event.get("data", {})
                    coin = stream_name.split("usdt@")[-1].upper()
                    if coin in prices:
                        prices[coin]["binance"] = float(data.get("a", 0.0))
        except Exception:
            await asyncio.sleep(2)

async def stream_hyperliquid():
    while True:
        try:
            async with websockets.connect(WS_URL) as websocket:
                subscription = {"method": "subscribe", "subscription": {"type": "allMids"}}
                await websocket.send(json.dumps(subscription))
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if "data" in data and "mids" in data["data"]:
                        for coin, price_str in data["data"]["mids"].items():
                            if coin in prices:
                                prices[coin]["hl"] = float(price_str)
        except Exception:
            await asyncio.sleep(2)

def calculate_position_size(info_client, account_address, current_price):
    try:
        user_state = info_client.user_state(account_address)
        margin_summary = user_state.get("marginSummary", {})
        account_value = float(margin_summary.get("accountValue", 0.0))
        if account_value <= 0:
            return 0.0
        allowed_loss_usd = account_value * RISK_PERCENT
        loss_per_unit = current_price * SL_PERCENT
        return round(allowed_loss_usd / loss_per_unit, 4)
    except Exception:
        return 0.0

async def master_arbitrage_engine():
    account = eth_account.Account.from_key(AGENT_KEY)
    info_client = Info(API_URL, skip_ws=True)
    exchange_client = Exchange(account, API_URL)
    
    asyncio.create_task(stream_binance())
    asyncio.create_task(stream_hyperliquid())
    print("🚀 1:3 Risk Engine Active. Scanning Matrix...")
    
    while True:
        current_time = time.time()
        for coin in COMMON_CRYPTO_UNIVERSE:
            coin_data = prices[coin]
            if current_time < coin_data["cooldown_until"]:
                continue
            b_price = coin_data["binance"]
            hl_price = coin_data["hl"]
            if b_price <= 0.0 or hl_price <= 0.0:
                continue
                
            spread_trigger = hl_price * 0.0015
            current_spread = b_price - hl_price
            
            if current_spread >= spread_trigger:
                size = calculate_position_size(info_client, account.address, hl_price)
                if size <= 0.0:
                    continue
                print(f"🔥 Lag Spotted on {coin}! Spread: ${current_spread:.4f}")
                try:
                    order_result = exchange_client.market_open(
                        name=coin, is_buy=True, size=size, px=hl_price, slippage=0.01
                    )
                    if order_result.get("status") == "ok":
                        print(f"✅ Market Entry Confirmed for {coin} at ${hl_price}")
                        stop_loss_price = round(hl_price * (1.0 - SL_PERCENT), 4)
                        take_profit_price = round(hl_price * (1.0 + TP_PERCENT), 4)
                        
                        exchange_client.order(
                            name=coin, is_buy=False, size=size, px=stop_loss_price,
                            order_type={"trigger": {"isTrigger": True, "triggerPx": stop_loss_price, "tpsl": "sl"}}
                        )
                        exchange_client.order(
                            name=coin, is_buy=False, size=size, px=take_profit_price,
                            order_type={"trigger": {"isTrigger": True, "triggerPx": take_profit_price, "tpsl": "tp"}}
                        )
                        print(f"🛡️ 1:3 Protection Placed. SL: {stop_loss_price} | TP: {take_profit_price}")
                        prices[coin]["cooldown_until"] = current_time + COOLDOWN_TIME
                except Exception as e:
                    print(f"❌ Execution layer anomaly: {e}")
        await asyncio.sleep(0.01)

if __name__ == "__main__":
    if not AGENT_KEY:
        print("❌ Error: Verification signature keys are missing inside your config variables.")
    else:
        asyncio.run(master_arbitrage_engine())
