// Copyright 2021 Optiver Asia Pacific Pty. Ltd.
//
// This file is part of Ready Trader Go.
//
//     Ready Trader Go is free software: you can redistribute it and/or
//     modify it under the terms of the GNU Affero General Public License
//     as published by the Free Software Foundation, either version 3 of
//     the License, or (at your option) any later version.
//
//     Ready Trader Go is distributed in the hope that it will be useful,
//     but WITHOUT ANY WARRANTY; without even the implied warranty of
//     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//     GNU Affero General Public License for more details.
//
//     You should have received a copy of the GNU Affero General Public
//     License along with Ready Trader Go.  If not, see
//     <https://www.gnu.org/licenses/>.
#include <array>
#include <chrono>
#include <cmath>

#include <boost/asio/io_context.hpp>

#include <ready_trader_go/logging.h>

#include "autotrader.h"

using namespace ReadyTraderGo;

RTG_INLINE_GLOBAL_LOGGER_WITH_CHANNEL(LG_AT, "AUTO")

constexpr int POSITION_LIMIT = 100;
constexpr int TICK_SIZE_IN_CENTS = 100;
constexpr int MAX_UNHEDGED_LOTS = 10;
constexpr int UNHEDGED_LOTS_TIME_LIMIT = 57500;
constexpr int MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) / TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS;
constexpr int MAX_ASK_NEAREST_TICK = MAXIMUM_ASK / TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS;

AutoTrader::AutoTrader(boost::asio::io_context& context) : BaseAutoTrader(context)
{
}

void AutoTrader::CancelAskOrder(unsigned long newAskPrice)
{
    if (mAskId != 0 && newAskPrice != 0 && newAskPrice != mAskPrice)
    {
        SendCancelOrder(mAskId);
        mAskId = 0;
    }
}

void AutoTrader::CancelBidOrder(unsigned long newBidPrice)
{
    if (mBidId != 0 && newBidPrice != 0 && newBidPrice != mBidPrice)
    {
        SendCancelOrder(mBidId);
        mBidId = 0;
    }
}

void AutoTrader::InsertAskOrder(unsigned long newAskPrice,
                                unsigned long newAskVolume)
{
    newAskVolume = std::min((long)(mPosition + POSITION_LIMIT - mAskVolume), (long)newAskVolume);
    if (mAskId == 0 && newAskPrice != 0 && newAskVolume > 0)
    {
        mAskId = mNextMessageId++;
        mAskPrice = newAskPrice;
        mAskVolume = newAskVolume;
        SendInsertOrder(mAskId, Side::SELL, newAskPrice, newAskVolume, Lifespan::GOOD_FOR_DAY);
        mAsks.emplace(mAskId);
    }
}

void AutoTrader::InsertBidOrder(unsigned long newBidPrice,
                                unsigned long newBidVolume)
{
    newBidVolume = std::min((long)(POSITION_LIMIT - mPosition - mBidVolume), (long)newBidVolume);
    if (mBidId == 0 && newBidPrice != 0 && newBidVolume > 0)
    {
        mBidId = mNextMessageId++;
        mBidPrice = newBidPrice;
        mBidVolume = newBidVolume;
        SendInsertOrder(mBidId, Side::BUY, newBidPrice, newBidVolume, Lifespan::GOOD_FOR_DAY);
        mBids.emplace(mBidId);
    }
}

void AutoTrader::HedgeOrder()
{
    if (mPosition + mHedgePosition > MAX_UNHEDGED_LOTS)
    {
        unsigned long askVolume = mPosition + mHedgePosition - MAX_UNHEDGED_LOTS;
        SendHedgeOrder(mNextMessageId++, Side::SELL, MIN_BID_NEAREST_TICK, askVolume);
        mHedgePosition -= askVolume;
    }
    else if (mPosition + mHedgePosition < -MAX_UNHEDGED_LOTS)
    {
        unsigned long bidVolume = - (mPosition + mHedgePosition + MAX_UNHEDGED_LOTS);
        SendHedgeOrder(mNextMessageId++, Side::BUY, MAX_ASK_NEAREST_TICK, bidVolume);
        mHedgePosition += bidVolume;
    }
}

void AutoTrader::DisconnectHandler()
{
    BaseAutoTrader::DisconnectHandler();
    RLOG(LG_AT, LogLevel::LL_INFO) << "execution connection lost";
}

void AutoTrader::ErrorMessageHandler(unsigned long clientOrderId,
                                     const std::string& errorMessage)
{
    RLOG(LG_AT, LogLevel::LL_INFO) << "error with order " << clientOrderId << ": " << errorMessage;
    if (clientOrderId != 0 && ((mAsks.count(clientOrderId) == 1) || (mBids.count(clientOrderId) == 1)))
    {
        OrderStatusMessageHandler(clientOrderId, 0, 0, 0);
    }
}

void AutoTrader::HedgeFilledMessageHandler(unsigned long clientOrderId,
                                           unsigned long price,
                                           unsigned long volume)
{
    RLOG(LG_AT, LogLevel::LL_INFO) << "hedge order " << clientOrderId << " filled for " << volume
                                   << " lots at $" << price << " average price in cents";
}

void AutoTrader::OrderBookMessageHandler(Instrument instrument,
                                         unsigned long sequenceNumber,
                                         const std::array<unsigned long, TOP_LEVEL_COUNT>& askPrices,
                                         const std::array<unsigned long, TOP_LEVEL_COUNT>& askVolumes,
                                         const std::array<unsigned long, TOP_LEVEL_COUNT>& bidPrices,
                                         const std::array<unsigned long, TOP_LEVEL_COUNT>& bidVolumes)
{
    RLOG(LG_AT, LogLevel::LL_INFO) << "order book received for " << instrument << " instrument"
                                   << ": ask prices: " << askPrices[0]
                                   << "; ask volumes: " << askVolumes[0]
                                   << "; bid prices: " << bidPrices[0]
                                   << "; bid volumes: " << bidVolumes[0];

    if (instrument == Instrument::FUTURE)
    {
        unsigned long newAskPrice = askPrices[0];
        unsigned long newBidPrice = bidPrices[0];

        CancelAskOrder(newAskPrice);
        InsertAskOrder(newAskPrice, mNewAskVolume);

        CancelBidOrder(newBidPrice);
        InsertBidOrder(newBidPrice, mNewBidVolume);

        if (std::abs(mPosition + mHedgePosition) <= MAX_UNHEDGED_LOTS)
        {
            mStartTime = std::chrono::steady_clock::now();
        }
        else if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - mStartTime).count() >= UNHEDGED_LOTS_TIME_LIMIT)
        {
            HedgeOrder();
            mStartTime = std::chrono::steady_clock::now();
        }
    }
}

void AutoTrader::OrderFilledMessageHandler(unsigned long clientOrderId,
                                           unsigned long price,
                                           unsigned long volume)
{
    RLOG(LG_AT, LogLevel::LL_INFO) << "order " << clientOrderId << " filled for " << volume
                                   << " lots at $" << price << " cents";
    if (mAsks.count(clientOrderId) == 1)
    {
        mPosition -= (long)volume;
    }
    else if (mBids.count(clientOrderId) == 1)
    {
        mPosition += (long)volume;
    }

    mNewBidVolume = (mPosition < 0) ? std::ceil((POSITION_LIMIT - mPosition) / 2) : std::floor((POSITION_LIMIT - mPosition) / 2);
    mNewAskVolume = mPosition + mNewBidVolume;
}

void AutoTrader::OrderStatusMessageHandler(unsigned long clientOrderId,
                                           unsigned long fillVolume,
                                           unsigned long remainingVolume,
                                           signed long fees)
{
    if (remainingVolume == 0)
    {
        if (clientOrderId == mAskId)
        {
            mAskId = 0;
            mAskVolume = 0;
        }
        else if (clientOrderId == mBidId)
        {
            mBidId = 0;
            mBidVolume = 0;
        }

        mAsks.erase(clientOrderId);
        mBids.erase(clientOrderId);
    }
    else if (fillVolume > 0)
    {
        if (clientOrderId == mAskId)
        {
            mAskVolume = std::max((long)(mAskVolume - fillVolume), 0L);
        }
        else if (clientOrderId == mBidId)
        {
            mBidVolume = std::max((long)(mBidVolume - fillVolume), 0L);
        }
    }
}

void AutoTrader::TradeTicksMessageHandler(Instrument instrument,
                                          unsigned long sequenceNumber,
                                          const std::array<unsigned long, TOP_LEVEL_COUNT>& askPrices,
                                          const std::array<unsigned long, TOP_LEVEL_COUNT>& askVolumes,
                                          const std::array<unsigned long, TOP_LEVEL_COUNT>& bidPrices,
                                          const std::array<unsigned long, TOP_LEVEL_COUNT>& bidVolumes)
{
    RLOG(LG_AT, LogLevel::LL_INFO) << "trade ticks received for " << instrument << " instrument"
                                   << ": ask prices: " << askPrices[0]
                                   << "; ask volumes: " << askVolumes[0]
                                   << "; bid prices: " << bidPrices[0]
                                   << "; bid volumes: " << bidVolumes[0];
}
