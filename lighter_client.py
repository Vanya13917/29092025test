import json
from eth_account import Account
from asyncio import sleep, Semaphore
from typing import Literal, Optional
from curl_cffi import AsyncSession as AsyncWebSocket

from lighter import (
    SignerClient, ApiClient, Configuration, AccountApi, create_api_key,
    OrderApi, AccountPosition, TransactionApi, ReferralApi
)
from modules.helpers.utils import get_proxy_url, logger, sleeping, calculate_pnl
from modules.helpers.retry import async_retry
from modules.data.constants import SIZE_DECIMALS, MARKET_IDS, PRICE_DECIMALS
from settings import ORDERS_TIMEOUT


INITIALIZATION_LOCK = Semaphore(15)


class LighterClient:
    API_URL = 'https://mainnet.zklighter.elliot.ai'
    AVERAGE_EXECUTION_PRICE_MULTIPLIER = 0.01

    def __init__(self, eth_private_key: str, proxy: str, name: str = None):
        self.eth_account = Account.from_key(eth_private_key)
        self.eth_address = self.eth_account.address
        self.name = name or self.eth_address[:8]
        self.eth_private_key = eth_private_key
        self.proxy = proxy
        
        self.account_index = None
        self.lighter_private_key = None
        self.api_client = ApiClient(
            configuration=Configuration(
                host=self.API_URL,
                proxy=get_proxy_url(self.proxy)
            )
        )
        self.account_api: Optional[AccountApi] = None
        self.order_api: Optional[OrderApi] = None
        self.signer_client: Optional[SignerClient] = None
        self.transaction_api: Optional[TransactionApi] = None
        self.referral_api: Optional[ReferralApi] = None

    async def close_sessions(self):
        if self.api_client:
            await self.api_client.close()

    async def _get_api_key(self, account_index, create_new: bool = False):
        try:
            with open('./database/cache.json') as f:
                cache = json.load(f)
                cached_info = cache.get('private_info', {}).get(self.eth_address)
        except (FileNotFoundError, json.JSONDecodeError):
            cached_info = None
            cache = {'private_info': {}}

        if cached_info and not create_new:
            lighter_private_key, account_index, public_key = cached_info
            signer_client = SignerClient(
                url=self.API_URL,
                private_key=lighter_private_key,
                api_key_index=2,
                account_index=account_index,
                api_client=self.api_client
            )
        else:
            lighter_private_key, public_key, err = create_api_key(account_index=account_index)
            if err is not None:
                raise Exception(f"Failed to create api key: {err}")

            signer_client = SignerClient(
                url=self.API_URL,
                private_key=lighter_private_key,
                api_key_index=2,
                account_index=self.account_index,
                api_client=self.api_client
            )

            response, err = await signer_client.change_api_key(
                eth_private_key=self.eth_private_key,
                new_pubkey=public_key
            )
            if err is not None:
                raise Exception(f"Failed to change api key: {err}")

            cache['private_info'][self.eth_address] = (lighter_private_key, self.account_index, public_key)
            with open('./database/cache.json', 'w') as f:
                json.dump(cache, f, indent=4)

            await sleep(10)

        err = signer_client.check_client()
        if err and cached_info and not create_new:
            logger.warning(f"{self.name} | API key expired, creating new one...")
            return await self._get_api_key(account_index, create_new=True)
        elif err:
            raise Exception(f"Failed to create new api key: {err}")
        elif not cached_info or create_new:
            logger.success(f"{self.name} | Created new API key")

        return signer_client, lighter_private_key, public_key

    @async_retry('Initialize')
    async def initialize(self):
        if self.signer_client:
            return

        async with INITIALIZATION_LOCK:

            self.account_api = AccountApi(self.api_client)
            self.order_api = OrderApi(self.api_client)
            self.transaction_api = TransactionApi(self.api_client)
            self.referral_api = ReferralApi(self.api_client)

            try:
                response = await self.account_api.accounts_by_l1_address(l1_address=self.eth_address)
                if not response.sub_accounts:
                    raise Exception(f"No accounts found for address {self.eth_address}")
                self.account_index = response.sub_accounts[0].index
            except Exception as e:
                raise Exception(f"Failed to get account info: {str(e)}")

            self.signer_client, self.lighter_private_key, _ = await self._get_api_key(self.account_index)

    async def get_auth_token(self):
        auth, err = self.signer_client.create_auth_token_with_expiry()
        if err is not None:
            raise Exception(f"Failed to create auth token: {err}")
        return auth

    async def _get_private_socket_info(self, channel: str):
        async with AsyncWebSocket(proxy=get_proxy_url(self.proxy), timeout=60) as s:
            ws = await s.ws_connect("wss://mainnet.zklighter.elliot.ai/stream")
            sub_data = {
                "type": "subscribe",
                "channel": f"{channel}/{self.account_index}",
                "auth": await self.get_auth_token()
            }
            await ws.send_json(sub_data)

            while True:
                reply = await ws.recv_json()
                if reply["type"] == f"subscribed/{channel}":
                    return reply

    @async_retry('Get Current Volume')
    async def get_current_volume(self) -> float:
        reply = await self._get_private_socket_info("account_all_trades")
        return round(float(reply["total_volume"]), 2)
    
    async def get_token_price(self, token: str) -> float:
        market_id = MARKET_IDS.get(token, token)
        r = await self.order_api.order_book_details(
            market_id,
        )
        return r.order_book_details[0].last_trade_price

    @async_retry('Create Market Order')
    async def create_market_order(
            self,
            token: str,
            side: Literal['long', 'short'],
            size: float = 0,
            usdc_size: float = 0,
            reduce_only: bool = False,
            debug_log: str = None,
            delay: float = 0
    ):
        await sleep(delay)
        if debug_log:
            logger.debug(debug_log)

        token_price = await self.get_token_price(token)

        multiplier = (1 + self.AVERAGE_EXECUTION_PRICE_MULTIPLIER) if side == 'long' else (1 - self.AVERAGE_EXECUTION_PRICE_MULTIPLIER)
        avg_execution_price = int(token_price * multiplier * 10 ** PRICE_DECIMALS[token])

        if size:
            base_amount = int(size * 10 ** SIZE_DECIMALS[token])
        else:
            base_amount = int(usdc_size / token_price * 10 ** SIZE_DECIMALS[token])

        r = await self.signer_client.create_market_order(
            market_index=MARKET_IDS.get(token, token),
            client_order_index=0,
            base_amount=base_amount,
            avg_execution_price=avg_execution_price,
            is_ask=side == 'short',
            reduce_only=reduce_only,
        )

        if r[2] is not None:
            raise Exception(f'Failed to create order: {r[2]}')

        return r

    @async_retry('Get Open Positions')
    async def get_open_positions(self) -> list[AccountPosition]:
        return await self.get_account_information('positions')
    
    async def close_all_positions(self):
        positions = await self.get_open_positions()
        closed = False
        
        # filter zero value positions
        positions = [position for position in positions if float(position.position) != 0]
        
        for position in positions:
            direction = 'long' if position.sign == 1 else 'short'
            
            logger.debug(f'{self.name} | Closing {direction} position {position.position} ${position.symbol} '
                         f'with {calculate_pnl(position)}% PNL')

            await self.create_market_order(
                token=position.symbol,
                side='long' if direction == 'short' else 'short',
                reduce_only=True
            )
            closed = True

            if position != positions[-1]:
                logger.success(f'{self.name} | Successfully closed position, waiting before next order...')
                await sleeping(ORDERS_TIMEOUT)
            else:
                logger.success(f'{self.name} | Successfully closed all positions on account!')
                
        return closed

    async def get_account_information(self, field: str):
        return (await self.account_api.account(
            by="index", value=str(self.account_index)
        )).accounts[0].__getattribute__(field)

    @async_retry('Get Balance')
    async def get_balance(self) -> float:
        return round(float(await self.get_account_information('collateral')), 2)

    @async_retry('Get Points')
    async def get_points(self) -> float:
        r = await self.referral_api.referral_points(self.account_index, authorization=await self.get_auth_token())
        return r.user_total_points

