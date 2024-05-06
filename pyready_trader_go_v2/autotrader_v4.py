# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader Go.
#
#     Ready Trader Go is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader Go is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader Go.  If not, see
#     <https://www.gnu.org/licenses/>.
import asyncio
import itertools
import math

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side

POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MAX_UNHEDGED_LOTS = 10
TICK_INTERVAL = 0.25
UNHEDGED_LOTS_TIME_LIMIT = 60 - 4 * TICK_INTERVAL
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS


class AutoTrader(BaseAutoTrader):
    """Example Auto-trader.

    When it starts this auto-trader places ten-lot bid and ask orders at the
    current best-bid and best-ask prices respectively. Thereafter, if it has
    a long position (it has bought more lots than it has sold) it reduces its
    bid and ask prices. Conversely, if it has a short position (it has sold
    more lots than it has bought) then it increases its bid and ask prices.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        """Initialise a new instance of the AutoTrader class."""
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bids = set()
        self.asks = set()
        self.ask_id = self.ask_price = self.ask_volume = self.bid_id = self.bid_price = self.bid_volume = self.position = self.hedge_position = 0
        self.start_time = loop.time()
        self.new_bid_volume = 50
        self.new_ask_volume = 50

    def cancel_bid_order(self, new_bid_price: int) -> None:
        if self.bid_id != 0 and new_bid_price not in (self.bid_price, 0):
            self.send_cancel_order(self.bid_id)
            self.bid_id = 0
    
    def cancel_ask_order(self, new_ask_price: int) -> None:
        if self.ask_id != 0 and new_ask_price not in (self.ask_price, 0):
            self.send_cancel_order(self.ask_id)
            self.ask_id = 0
    
    def insert_bid_order(self, bid_price: int, bid_volume: int, lifespan: Lifespan) -> None:
        bid_volume = min(POSITION_LIMIT - self.position - self.bid_volume, bid_volume)
        if self.bid_id == 0 and bid_price != 0 and bid_volume > 0:
            self.bid_id = next(self.order_ids)
            self.bid_price = bid_price
            self.bid_volume = bid_volume
            self.send_insert_order(self.bid_id, Side.BUY, bid_price, bid_volume, lifespan)
            self.bids.add(self.bid_id)
    
    def insert_ask_order(self, ask_price: int, ask_volume: int, lifespan: Lifespan) -> None:
        ask_volume = min(self.position + POSITION_LIMIT - self.ask_volume, ask_volume)
        if self.ask_id == 0 and ask_price != 0 and ask_volume > 0:
            self.ask_id = next(self.order_ids)
            self.ask_price = ask_price
            self.ask_volume = ask_volume
            self.send_insert_order(self.ask_id, Side.SELL, ask_price, ask_volume, lifespan)
            self.asks.add(self.ask_id)

    def hedge_order(self):
        if self.position + self.hedge_position > MAX_UNHEDGED_LOTS:
            ask_volume = self.position + self.hedge_position
            self.send_hedge_order(next(self.order_ids), Side.ASK, MIN_BID_NEAREST_TICK, ask_volume)
            self.hedge_position -= ask_volume
        elif self.position + self.hedge_position < -MAX_UNHEDGED_LOTS:
            bid_volume = - (self.position + self.hedge_position)
            self.send_hedge_order(next(self.order_ids), Side.BID, MAX_ASK_NEAREST_TICK, bid_volume)
            self.hedge_position += bid_volume

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())
        if client_order_id != 0 and (client_order_id in self.bids or client_order_id in self.asks):
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id,
                         price, volume)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        self.logger.info("received order book for instrument %d with sequence number %d", instrument,
                         sequence_number)

        if instrument == Instrument.FUTURE:
            new_bid_price = bid_prices[0] - TICK_SIZE_IN_CENTS if bid_prices[0] != 0 else 0
            new_ask_price = ask_prices[0] + TICK_SIZE_IN_CENTS if ask_prices[0] != 0 else 0
            
            self.cancel_bid_order(new_bid_price)
            self.insert_bid_order(new_bid_price, self.new_bid_volume, Lifespan.GOOD_FOR_DAY)
            
            self.cancel_ask_order(new_ask_price)
            self.insert_ask_order(new_ask_price, self.new_ask_volume, Lifespan.GOOD_FOR_DAY)

            if abs(self.position + self.hedge_position) <= MAX_UNHEDGED_LOTS:
                self.start_time = self.event_loop.time()
            elif self.event_loop.time() - self.start_time >= UNHEDGED_LOTS_TIME_LIMIT:
                self.hedge_order()
                self.start_time = self.event_loop.time()
    
    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
                         price, volume)
        if client_order_id in self.bids:
            self.position += volume
        elif client_order_id in self.asks:
            self.position -= volume

        self.new_bid_volume = math.ceil((POSITION_LIMIT - self.position) / 2) if self.position < 0 else math.floor((POSITION_LIMIT - self.position) / 2)
        self.new_ask_volume = POSITION_LIMIT - self.new_bid_volume

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)
        if remaining_volume == 0:
            if client_order_id == self.bid_id:
                self.bid_id = 0
                self.bid_volume = 0
            elif client_order_id == self.ask_id:
                self.ask_id = 0
                self.ask_volume = 0

            # It could be either a bid or an ask
            self.bids.discard(client_order_id)
            self.asks.discard(client_order_id)
        elif fill_volume > 0:
            if client_order_id == self.bid_id:
                self.bid_volume = max(self.bid_volume - fill_volume, 0)
            elif client_order_id == self.ask_id:
                self.ask_volume = max(self.ask_volume - fill_volume, 0)

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically when there is trading activity on the market.

        The five best ask (i.e. sell) and bid (i.e. buy) prices at which there
        has been trading activity are reported along with the aggregated volume
        traded at each of those price levels.

        If there are less than five prices on a side, then zeros will appear at
        the end of both the prices and volumes arrays.
        """
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
                         sequence_number)
