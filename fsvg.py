#!/usr/bin/env python3
"""
FSVG – Funding-Subsidised Volatility-Grid
Binance ETH-USDT perpetual, isolated 3×, delta-neutral.
< 250 lines, asyncio, zero TA libs.
"""
import asyncio, json, time, os, logging, sys
from datetime import datetime, timedelta
import aiohttp, websockets, pandas as pd, numpy as np
from decimal import Decimal, ROUND_DOWN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("logs/fsvg.log"), logging.StreamHandler()],
)

with open("params.json") as f:
    P = json.load(f)

SYMBOL = P["symbol"]
LEV, EQ, MAX_NOTIONAL = P["lev"], Decimal(str(P["base_equity"])), Decimal(str(P["max_notional"]))
GAMMA, N, INV_TRIG = Decimal(str(P["gamma"])), P["n_orders"], Decimal(str(P["inv_trigger"]))
REFRESH, HEDGE, KILL_DD = P["grid_refresh_sec"], P["hedge_sec"], Decimal(str(P["kill_dd"]))
MIN_LOT = Decimal(str(P["min_lot"]))

API_KEY = os.getenv("BINANCE_KEY")
SECRET = os.getenv("BINANCE_SECRET")
TESTNET = os.getenv("TESTNET", "true").lower() == "true"

REST = (
    "https://testnet.binancefuture.com/fapi/v1"
    if TESTNET
    else "https://fapi.binance.com/fapi/v1"
)
WS = (
    "wss://stream.binancefuture.com/ws"
    if TESTNET
    else "wss://fstream.binance.com/ws"
)


# ---------- sig helpers ----------
def sign(qs: str) -> str:
    import hmac, hashlib

    return hmac.new(SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()


def fmt(x: Decimal) -> str:
    return f"{x.quantize(MIN_LOT, rounding=ROUND_DOWN)}"


# ---------- state ----------
state = {
    "mid": Decimal("0"),
    "sigma24": Decimal("0"),
    "inv": Decimal("0"),  # base (ETH) inventory
    "eq": EQ,
    "orders": {},  # clientOrderId -> price
    "last_pnl": 0,
}


# ---------- REST ----------
async def rest(path, params=None, signed=False):
    async with aiohttp.ClientSession() as s:
        if params is None:
            params = {}
        params["timestamp"] = int(time.time() * 1000)
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        if signed:
            params["signature"] = sign(qs)
        async with s.get(REST + path, params=params, headers={"X-MEX-APIKEY": API_KEY}) as r:
            return await r.json()


async def get_exch_info():
    info = await rest("/exchangeInfo")
    for s in info["symbols"]:
        if s["symbol"] == SYMBOL:
            return s
    raise RuntimeError("symbol not found")


async def account_snapshot():
    acc = await rest("/account", signed=True)
    for p in acc["positions"]:
        if p["symbol"] == SYMBOL:
            return p
    raise RuntimeError("position not found")


async def send_order(side: str, price: Decimal, qty: Decimal, client_id: str):
    params = {
        "symbol": SYMBOL,
        "side": side,
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": fmt(qty),
        "price": str(price),
        "newClientOrderId": client_id,
    }
    return await rest("/order", params, signed=True)


async def market_order(side: str, qty: Decimal):
    params = {"symbol": SYMBOL, "side": side, "type": "MARKET", "quantity": fmt(qty)}
    return await rest("/order", params, signed=True)


async def cancel_all():
    await rest("/allOpenOrders", {"symbol": SYMBOL}, signed=True)


# ---------- WS ----------
async def listen_ws():
    async with websockets.connect(f"{WS}/{SYMBOL.lower()}@bookTicker") as ws:
        async for msg in ws:
            d = json.loads(msg)
            state["mid"] = (Decimal(d["bidPrice"]) + Decimal(d["askPrice"])) / 2


async def listen_kline():
    async with websockets.connect(f"{WS}/{SYMBOL.lower()}@kline_5m") as ws:
        async for msg in ws:
            d = json.loads(msg)["k"]
            closes.append(float(d["c"]))
            if len(closes) > 288:
                closes.pop(0)
            if len(closes) == 288:
                state["sigma24"] = Decimal(
                    np.log(pd.Series(closes)).diff().std() * np.sqrt(288 * 24 * 365)
                )


# ---------- core ----------
closes = []


async def vol_loop():
    while True:
        try:
            await listen_kline()
        except Exception as e:
            logging.exception("kline ws")
            await asyncio.sleep(5)


async def price_loop():
    while True:
        try:
            await listen_ws()
        except Exception as e:
            logging.exception("book ws")
            await asyncio.sleep(5)


def grid_prices(mid, sigma):
    gap = GAMMA * sigma * mid
    ticks = np.linspace(-N // 2, N // 2, N)
    buys = [mid + Decimal(str(t)) * gap / (N // 2) for t in ticks if t < 0]
    sells = [mid + Decimal(str(t)) * gap / (N // 2) for t in ticks if t > 0]
    return buys, sells


async def refresh_grid():
    while True:
        try:
            mid = state["mid"]
            sigma = state["sigma24"] or Decimal("0.8")  # fallback 80 % IV
            if mid == 0:
                await asyncio.sleep(1)
                continue
            buys, sells = grid_prices(mid, sigma)
            await cancel_all()
            state["orders"].clear()
            quote_per_order = MAX_NOTIONAL / N
            qty = quote_per_order / mid
            for p in buys:
                cid = f"buy-{int(time.time()*1000)}-{p}"
                await send_order("BUY", p, qty, cid)
                state["orders"][cid] = p
            for p in sells:
                cid = f"sell-{int(time.time()*1000)}-{p}"
                await send_order("SELL", p, qty, cid)
                state["orders"][cid] = p
            logging.info("grid refreshed @ mid=%s σ=%s %s orders", mid, sigma, len(state["orders"]))
        except Exception as e:
            logging.exception("refresh")
        await asyncio.sleep(REFRESH)


async def hedge_loop():
    while True:
        try:
            pos = await account_snapshot()
            inv = Decimal(pos["positionAmt"])
            state["inv"] = inv
            eq = Decimal(pos["unrealizedProfit"]) + Decimal(pos["isolatedWallet"])
            state["eq"] = eq
            if abs(inv) > INV_TRIG:
                side = "SELL" if inv > 0 else "BUY"
                await market_order(side, abs(inv))
                logging.warning("hedge %s %s", side, abs(inv))
            dd = (EQ - state["eq"]) / EQ
            if dd > KILL_DD:
                logging.error("kill-switch %.2f", dd)
                await cancel_all()
                await market_order("SELL" if inv > 0 else "BUY", abs(inv))
                sys.exit(1)
        except Exception as e:
            logging.exception("hedge")
        await asyncio.sleep(HEDGE)


# ---------- main ----------
async def main():
    tasks = [asyncio.create_task(price_loop()), asyncio.create_task(vol_loop()), asyncio.create_task(refresh_grid()), asyncio.create_task(hedge_loop())]
    await asyncio.gather(*tasks)


if _name_ == "_main_":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("stopped by user")
