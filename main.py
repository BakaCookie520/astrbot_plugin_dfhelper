from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import aiohttp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star


API_BASE_URL = "https://orzice.com/workApi"
DIY_API_PATH = "/v1/sjz_api/jzv3_diy"
MARKET_API_PATH = "/v1/sjz_api/jzv3_zb"
MAP_PWD_API_PATH = "/v1/sjz_api/map_pwd"
MANUFACTURE_API_PATH = "/v1/sjz_api/manufacturePro"
ITEM_INFO_API_PATH = "/v1/sjz_api/item_info_all"
ITEM_PRICE_API_PATH = "/v1/sjz_api/item_price_all"
MIN_ZB = 1500
MAX_ZB = 780000
PENDING_EXPIRE_SECONDS = 60
CACHE_FILE_NAME = "item_info_cache.json"

LV_OPTIONS = {
    0: "11W 机密配置",
    1: "18W 机密配置",
    2: "55W 绝密巴克什",
    3: "60W 绝密航天",
    4: "24W 适应监狱（每周五12:00 ~ 周一00:00 开启才计算）",
    5: "78W 绝密监狱",
}

MAP_NAMES = {
    "a": "零号大坝",
    "b": "长弓溪谷",
    "c": "巴克什",
    "d": "航天基地",
    "e": "潮汐监狱",
}

MANUFACTURE_TYPES = {
    1: "技术中心",
    2: "工作台",
    3: "制药台",
    4: "防具台",
}

MANUFACTURE_TYPE_ALIASES = {
    "技术中心": 1,
    "技术": 1,
    "jszx": 1,
    "js": 1,
    "工作台": 2,
    "工作": 2,
    "gzt": 2,
    "gz": 2,
    "制药台": 3,
    "制药": 3,
    "zyt": 3,
    "zy": 3,
    "防具台": 4,
    "防具": 4,
    "fjt": 4,
    "fj": 4,
}

GEAR_OPTIONS = {
    "is_bb": "背包",
    "is_gun": "枪械和配件",
    "is_hj": "护甲",
    "is_sq": "手枪",
    "is_tk": "头盔",
    "is_xg": "胸挂",
}

OPTION_ALIASES = {
    "背包": "is_bb",
    "beibao": "is_bb",
    "bb": "is_bb",
    "is_bb": "is_bb",
    "枪": "is_gun",
    "枪械": "is_gun",
    "qx": "is_gun",
    "qiang": "is_gun",
    "qiangxie": "is_gun",
    "gun": "is_gun",
    "is_gun": "is_gun",
    "护甲": "is_hj",
    "甲": "is_hj",
    "hujia": "is_hj",
    "jia": "is_hj",
    "hj": "is_hj",
    "is_hj": "is_hj",
    "手枪": "is_sq",
    "shouqiang": "is_sq",
    "sq": "is_sq",
    "is_sq": "is_sq",
    "头盔": "is_tk",
    "盔": "is_tk",
    "toukui": "is_tk",
    "kui": "is_tk",
    "tk": "is_tk",
    "is_tk": "is_tk",
    "胸挂": "is_xg",
    "xionggua": "is_xg",
    "xg": "is_xg",
    "is_xg": "is_xg",
}

MODE_ALIASES = {
    "需求": "需求",
    "xq": "需求",
    "xuqiu": "需求",
    "已有": "已有",
    "yy": "已有",
    "yiyou": "已有",
}

FEATURE_LABELS = {
    "market": "卡战备",
    "custom": "自定义卡战备",
    "password": "今日密码",
    "manufacture": "制造利润",
    "trade": "交易行查询",
}

FEATURE_ALIASES = {
    "卡战备": "market",
    "kzb": "market",
    "market": "market",
    "自定义卡战备": "custom",
    "自定义战备": "custom",
    "zdykzb": "custom",
    "custom": "custom",
    "今日密码": "password",
    "密码": "password",
    "jrmm": "password",
    "password": "password",
    "制造利润": "manufacture",
    "zzlr": "manufacture",
    "manufacture": "manufacture",
    "交易行查询": "trade",
    "交易行": "trade",
    "物价": "trade",
    "查物价": "trade",
    "jyh": "trade",
    "trade": "trade",
}


class DfHelperPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.pending_lv_queries: dict[str, float] = {}
        self.item_info_cache_path = Path(__file__).with_name(CACHE_FILE_NAME)
        self.item_info_items: list[dict[str, Any]] = []
        self.item_info_loaded = False
        self.item_info_load_error = ""
        self.item_info_lock = asyncio.Lock()
        self.item_price_cache: dict[int, dict[str, Any]] = {}
        self.item_price_cache_at = 0.0
        try:
            asyncio.get_running_loop().create_task(self._ensure_item_info_loaded())
        except RuntimeError:
            logger.warning("交易行基础信息库未能在插件初始化时预加载：当前无线程事件循环。")

    @filter.command("三角洲帮助", alias={"df_help", "df_bz"})
    async def help(self, event: AstrMessageEvent):
        """列出插件所有可用命令。"""
        if denial := self._check_acl(event):
            yield event.plain_result(denial)
            return

        yield event.plain_result(self._help_text())

    @filter.command("添加次数", alias={"tjcs"})
    async def add_usage_quota(self, event: AstrMessageEvent):
        """管理员为用户添加指定功能使用次数。"""
        if denial := self._check_acl(event):
            yield event.plain_result(denial)
            return
        if not self._is_admin(event):
            yield event.plain_result("只有管理员可以添加使用次数。")
            return

        try:
            user_id, feature_key, amount = self._parse_add_quota_query(event.message_str)
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return

        total = self._add_usage_quota(user_id, feature_key, amount)
        yield event.plain_result(
            f"已为用户 {user_id} 添加 {amount} 次{FEATURE_LABELS[feature_key]}使用次数，当前剩余 {total} 次。"
        )

    @filter.command("查询次数", alias={"cxcs"})
    async def query_usage_quota(self, event: AstrMessageEvent):
        """查询用户功能使用次数。"""
        if denial := self._check_acl(event):
            yield event.plain_result(denial)
            return

        args = self._strip_command(event.message_str)
        target_user_id = str(event.get_sender_id() or "").strip()
        if args:
            if not self._is_admin(event):
                yield event.plain_result("只有管理员可以查询其他用户的使用次数。")
                return
            target_user_id = args[0].strip()

        if not target_user_id:
            yield event.plain_result("查询失败：无法获取用户 ID。")
            return

        yield event.plain_result(self._format_usage_quota(target_user_id))

    @filter.command("自定义卡战备", alias={"自定义战备", "diy卡战备", "zdykzb"})
    async def custom_kazhanbei(self, event: AstrMessageEvent):
        """查询三角洲行动自定义卡战备方案。"""
        if denial := self._check_access(event, "custom"):
            yield event.plain_result(denial)
            return

        try:
            params = self._parse_query(event.message_str)
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return

        api_token = str(self.config.get("api_token", "")).strip()
        timeout_seconds = int(self.config.get("timeout_seconds", 8) or 8)

        if not api_token:
            yield event.plain_result("卡战备查询失败：请先在插件配置中填写 api_token。")
            return

        params["token"] = api_token
        endpoint = f"{API_BASE_URL}{DIY_API_PATH}"

        try:
            payload = await self._request_api(endpoint, params, timeout_seconds)
        except asyncio.TimeoutError:
            yield event.plain_result("卡战备查询超时，请稍后重试。")
            return
        except aiohttp.ClientError as exc:
            logger.warning("卡战备接口网络异常: %s", exc)
            yield event.plain_result("卡战备查询失败：网络异常或接口不可达。")
            return
        except ValueError as exc:
            yield event.plain_result(f"卡战备查询失败：{exc}")
            return

        if payload.get("code") != 0:
            message = payload.get("message") or payload.get("msg") or "接口返回错误"
            yield event.plain_result(f"卡战备查询失败：{message}")
            return

        remaining = self._consume_usage_quota(event, "custom")
        if remaining == -1:
            yield event.plain_result(self._quota_denial("custom"))
            return

        try:
            yield event.plain_result(self._append_remaining(self._format_custom_result(payload, params["zb"]), remaining))
        except ValueError as exc:
            yield event.plain_result(f"卡战备查询失败：{exc}")

    @filter.command("卡战备", alias={"kzb"})
    async def kazhanbei(self, event: AstrMessageEvent):
        """展示实时卡战备数字参数，并等待用户回复数字查询。"""
        if denial := self._check_access(event, "market"):
            yield event.plain_result(denial)
            return

        self.pending_lv_queries[self._pending_key(event)] = time.monotonic()
        yield event.plain_result(self._lv_usage())

    @filter.command("今日密码", alias={"密码", "地图密码", "保险密码", "jrmm"})
    async def today_password(self, event: AstrMessageEvent):
        """查询今日地图密码。"""
        if denial := self._check_access(event, "password"):
            yield event.plain_result(denial)
            return

        api_token = str(self.config.get("api_token", "")).strip()
        timeout_seconds = int(self.config.get("timeout_seconds", 8) or 8)

        if not api_token:
            yield event.plain_result("今日密码查询失败：请先在插件配置中填写 api_token。")
            return

        endpoint = f"{API_BASE_URL}{MAP_PWD_API_PATH}"
        params = {"token": api_token}

        try:
            payload = await self._request_api(endpoint, params, timeout_seconds)
        except asyncio.TimeoutError:
            yield event.plain_result("今日密码查询超时，请稍后重试。")
            return
        except aiohttp.ClientError as exc:
            logger.warning("今日密码接口网络异常: %s", exc)
            yield event.plain_result("今日密码查询失败：网络异常或接口不可达。")
            return
        except ValueError as exc:
            yield event.plain_result(f"今日密码查询失败：{exc}")
            return

        if payload.get("code") != 0:
            message = payload.get("message") or payload.get("msg") or "接口返回错误"
            yield event.plain_result(f"今日密码查询失败：{message}")
            return

        remaining = self._consume_usage_quota(event, "password")
        if remaining == -1:
            yield event.plain_result(self._quota_denial("password"))
            return

        try:
            yield event.plain_result(self._append_remaining(self._format_map_password(payload), remaining))
        except ValueError as exc:
            yield event.plain_result(f"今日密码查询失败：{exc}")

    @filter.command("制造利润", alias={"zzlr"})
    async def manufacture_profit(self, event: AstrMessageEvent):
        """查询特勤处制造利润。"""
        if denial := self._check_access(event, "manufacture"):
            yield event.plain_result(denial)
            return

        try:
            params = self._parse_manufacture_query(event.message_str)
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return

        api_token = str(self.config.get("api_token", "")).strip()
        timeout_seconds = int(self.config.get("timeout_seconds", 8) or 8)

        if not api_token:
            yield event.plain_result("制造利润查询失败：请先在插件配置中填写 api_token。")
            return

        endpoint = f"{API_BASE_URL}{MANUFACTURE_API_PATH}"
        params["token"] = api_token

        try:
            payload = await self._request_api(endpoint, params, timeout_seconds)
        except asyncio.TimeoutError:
            yield event.plain_result("制造利润查询超时，请稍后重试。")
            return
        except aiohttp.ClientError as exc:
            logger.warning("制造利润接口网络异常: %s", exc)
            yield event.plain_result("制造利润查询失败：网络异常或接口不可达。")
            return
        except ValueError as exc:
            yield event.plain_result(f"制造利润查询失败：{exc}")
            return

        if payload.get("code") != 0:
            message = payload.get("message") or payload.get("msg") or "接口返回错误"
            yield event.plain_result(f"制造利润查询失败：{message}")
            return

        remaining = self._consume_usage_quota(event, "manufacture")
        if remaining == -1:
            yield event.plain_result(self._quota_denial("manufacture"))
            return

        try:
            yield event.plain_result(
                self._append_remaining(self._format_manufacture_profit(payload, params["t"], params["l"]), remaining)
            )
        except ValueError as exc:
            yield event.plain_result(f"制造利润查询失败：{exc}")

    @filter.command("交易行", alias={"查物价", "物价", "jyh"})
    async def trade_item(self, event: AstrMessageEvent):
        """查询交易行物品最新价格。"""
        if denial := self._check_access(event, "trade"):
            yield event.plain_result(denial)
            return

        try:
            keyword = self._parse_trade_query(event.message_str)
        except ValueError as exc:
            yield event.plain_result(str(exc))
            return

        api_token = str(self.config.get("api_token", "")).strip()
        timeout_seconds = int(self.config.get("timeout_seconds", 8) or 8)

        if not api_token:
            yield event.plain_result("交易行查询失败：请先在插件配置中填写 api_token。")
            return

        try:
            await self._ensure_item_info_loaded()
        except asyncio.TimeoutError:
            yield event.plain_result("交易行基础信息库加载超时，请稍后重试。")
            return
        except aiohttp.ClientError as exc:
            logger.warning("交易行基础信息库接口网络异常: %s", exc)
            yield event.plain_result("交易行基础信息库加载失败：网络异常或接口不可达。")
            return
        except ValueError as exc:
            yield event.plain_result(f"交易行基础信息库加载失败：{exc}")
            return

        if not self.item_info_items:
            detail = f"：{self.item_info_load_error}" if self.item_info_load_error else ""
            yield event.plain_result(f"交易行基础信息库为空{detail}。")
            return

        matches = self._search_trade_items(keyword)
        if not matches:
            yield event.plain_result(f"未找到名称包含“{keyword}”的物品。")
            return

        endpoint = f"{API_BASE_URL}{ITEM_PRICE_API_PATH}"
        params = {"token": api_token}

        try:
            prices = await self._get_item_prices(endpoint, params, timeout_seconds)
        except asyncio.TimeoutError:
            yield event.plain_result("交易行价格查询超时，请稍后重试。")
            return
        except aiohttp.ClientError as exc:
            logger.warning("交易行价格接口网络异常: %s", exc)
            yield event.plain_result("交易行价格查询失败：网络异常或接口不可达。")
            return
        except ValueError as exc:
            yield event.plain_result(f"交易行价格查询失败：{exc}")
            return

        remaining = self._consume_usage_quota(event, "trade")
        if remaining == -1:
            yield event.plain_result(self._quota_denial("trade"))
            return

        yield event.plain_result(self._append_remaining(self._format_trade_items(keyword, matches, prices), remaining))

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_waiting_lv_reply(self, event: AstrMessageEvent):
        """处理 /卡战备 后用户回复的 lv 数字。"""
        message = event.message_str.strip()
        if not message or message.startswith("/"):
            return

        key = self._pending_key(event)
        started_at = self.pending_lv_queries.get(key)
        if started_at is None:
            return

        event.stop_event()

        if denial := self._check_access(event, "market"):
            self.pending_lv_queries.pop(key, None)
            yield event.plain_result(denial)
            return

        if time.monotonic() - started_at > PENDING_EXPIRE_SECONDS:
            self.pending_lv_queries.pop(key, None)
            yield event.plain_result("卡战备查询已超时，请重新发送 /卡战备。")
            return

        if message in {"取消", "cancel", "退出"}:
            self.pending_lv_queries.pop(key, None)
            yield event.plain_result("已取消卡战备查询。")
            return

        try:
            lv = int(message)
        except ValueError:
            yield event.plain_result("请回复 0 到 5 的数字参数，或回复“取消”。")
            return

        if lv not in LV_OPTIONS:
            yield event.plain_result("参数无效，请回复 0 到 5 的数字参数，或回复“取消”。")
            return

        self.pending_lv_queries.pop(key, None)
        api_token = str(self.config.get("api_token", "")).strip()
        timeout_seconds = int(self.config.get("timeout_seconds", 8) or 8)

        if not api_token:
            yield event.plain_result("卡战备查询失败：请先在插件配置中填写 api_token。")
            return

        endpoint = f"{API_BASE_URL}{MARKET_API_PATH}"
        params = {"lv": lv, "token": api_token}

        try:
            payload = await self._request_api(endpoint, params, timeout_seconds)
        except asyncio.TimeoutError:
            yield event.plain_result("卡战备查询超时，请稍后重试。")
            return
        except aiohttp.ClientError as exc:
            logger.warning("实时卡战备接口网络异常: %s", exc)
            yield event.plain_result("卡战备查询失败：网络异常或接口不可达。")
            return
        except ValueError as exc:
            yield event.plain_result(f"卡战备查询失败：{exc}")
            return

        if payload.get("code") != 0:
            message = payload.get("message") or payload.get("msg") or "接口返回错误"
            yield event.plain_result(f"卡战备查询失败：{message}")
            return

        remaining = self._consume_usage_quota(event, "market")
        if remaining == -1:
            yield event.plain_result(self._quota_denial("market"))
            return

        try:
            yield event.plain_result(self._append_remaining(self._format_market_result(payload, lv), remaining))
        except ValueError as exc:
            yield event.plain_result(f"卡战备查询失败：{exc}")

    async def _request_api(
        self,
        endpoint: str,
        params: dict[str, Any],
        timeout_seconds: int,
    ) -> dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=max(timeout_seconds, 1))
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(endpoint, params=params) as response:
                if response.status != 200:
                    raise ValueError(f"接口 HTTP 状态码 {response.status}")
                try:
                    payload = await response.json(content_type=None)
                except Exception as exc:
                    raise ValueError("接口返回不是有效 JSON") from exc

        if not isinstance(payload, dict):
            raise ValueError("接口返回结构异常")
        return payload

    async def _ensure_item_info_loaded(self):
        if self.item_info_loaded:
            return

        async with self.item_info_lock:
            if self.item_info_loaded:
                return

            cached_items = self._load_item_info_cache()
            if cached_items:
                self.item_info_items = cached_items
                self.item_info_loaded = True
                return

            api_token = str(self.config.get("api_token", "")).strip()
            if not api_token:
                self.item_info_load_error = "请先在插件配置中填写 api_token"
                return

            timeout_seconds = int(self.config.get("timeout_seconds", 8) or 8)
            endpoint = f"{API_BASE_URL}{ITEM_INFO_API_PATH}"
            payload = await self._request_api(endpoint, {"token": api_token}, timeout_seconds)
            if payload.get("code") != 0:
                message = payload.get("message") or payload.get("msg") or "接口返回错误"
                self.item_info_load_error = str(message)
                raise ValueError(message)

            items = self._extract_item_list(payload, "基础信息库")
            if not items:
                self.item_info_load_error = "接口未返回物品数据"
                raise ValueError(self.item_info_load_error)

            self.item_info_items = items
            self.item_info_loaded = True
            self.item_info_load_error = ""
            self._save_item_info_cache(items)

    def _load_item_info_cache(self) -> list[dict[str, Any]]:
        if not self.item_info_cache_path.exists():
            return []

        try:
            with self.item_info_cache_path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("读取交易行基础信息缓存失败: %s", exc)
            return []

        items = payload.get("items") if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            return []

        return [item for item in items if isinstance(item, dict)]

    def _save_item_info_cache(self, items: list[dict[str, Any]]):
        payload = {
            "cached_at": int(time.time()),
            "items": items,
        }
        try:
            with self.item_info_cache_path.open("w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, separators=(",", ":"))
        except OSError as exc:
            logger.warning("保存交易行基础信息缓存失败: %s", exc)

    async def _get_item_prices(
        self,
        endpoint: str,
        params: dict[str, Any],
        timeout_seconds: int,
    ) -> dict[int, dict[str, Any]]:
        ttl_seconds = int(self.config.get("item_price_cache_seconds", 45) or 45)
        ttl_seconds = max(0, min(ttl_seconds, 300))
        now = time.monotonic()
        if self.item_price_cache and ttl_seconds and now - self.item_price_cache_at <= ttl_seconds:
            return self.item_price_cache

        payload = await self._request_api(endpoint, params, timeout_seconds)
        if payload.get("code") != 0:
            message = payload.get("message") or payload.get("msg") or "接口返回错误"
            raise ValueError(message)

        items = self._extract_item_list(payload, "价格库")
        prices: dict[int, dict[str, Any]] = {}
        for item in items:
            item_id = self._first_int(item, ("id", "oid", "objectID"))
            if item_id is not None:
                prices[item_id] = item

        if not prices:
            raise ValueError("接口未返回可用价格数据")

        self.item_price_cache = prices
        self.item_price_cache_at = now
        return prices

    def _extract_item_list(self, payload: dict[str, Any], label: str) -> list[dict[str, Any]]:
        data = payload.get("data")
        candidates = [data]
        if isinstance(data, dict):
            candidates.extend(data.get(key) for key in ("data", "list", "items", "rows"))

        for candidate in candidates:
            if isinstance(candidate, list):
                return [item for item in candidate if isinstance(item, dict)]

        raise ValueError(f"{label} data 字段结构异常")

    def _check_acl(self, event: AstrMessageEvent) -> str | None:
        user_id = str(event.get_sender_id() or "").strip()
        group_id = str(getattr(getattr(event, "message_obj", None), "group_id", "") or "").strip()

        user_blacklist = self._config_id_set("user_blacklist")
        group_blacklist = self._config_id_set("group_blacklist")

        if user_id and user_id in user_blacklist:
            return "你在用户黑名单中，无法使用该功能。"
        if group_id and group_id in group_blacklist:
            return "当前群在群黑名单中，无法使用该功能。"

        return None

    def _check_access(self, event: AstrMessageEvent, feature_key: str) -> str | None:
        if denial := self._check_acl(event):
            return denial
        if self._is_quota_exempt(event):
            return None

        user_id = str(event.get_sender_id() or "").strip()
        if not user_id:
            return "无法获取用户 ID，无法检查使用资格。"

        if self._get_usage_quota(user_id, feature_key) <= 0:
            return self._quota_denial(feature_key)
        return None

    def _is_quota_exempt(self, event: AstrMessageEvent) -> bool:
        user_id = str(event.get_sender_id() or "").strip()
        group_id = str(getattr(getattr(event, "message_obj", None), "group_id", "") or "").strip()
        return (
            self._is_admin(event)
            or (user_id and user_id in self._config_id_set("user_whitelist"))
            or (group_id and group_id in self._config_id_set("group_whitelist"))
        )

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        user_id = str(event.get_sender_id() or "").strip()
        return bool(user_id and user_id in self._config_id_set("admin_user_ids"))

    def _quota_denial(self, feature_key: str) -> str:
        label = FEATURE_LABELS.get(feature_key, feature_key)
        return f"你没有{label}的使用次数，请联系管理员添加。"

    def _append_remaining(self, message: str, remaining: int | None) -> str:
        if remaining is None:
            return message
        return f"{message}\n剩余使用次数：{remaining}"

    def _get_usage_store(self) -> dict[str, Any]:
        store = self.config.get("usage_quotas", {})
        if not isinstance(store, dict):
            store = {}
            self.config["usage_quotas"] = store
        return store

    def _get_user_usage_store(self, user_id: str) -> dict[str, int]:
        store = self._get_usage_store()
        user_store = store.get(user_id)
        if not isinstance(user_store, dict):
            user_store = {}
            store[user_id] = user_store
        return user_store

    def _get_usage_quota(self, user_id: str, feature_key: str) -> int:
        value = self._get_user_usage_store(user_id).get(feature_key, 0)
        return value if isinstance(value, int) else 0

    def _add_usage_quota(self, user_id: str, feature_key: str, amount: int) -> int:
        user_store = self._get_user_usage_store(user_id)
        total = max(0, self._get_usage_quota(user_id, feature_key) + amount)
        user_store[feature_key] = total
        self._save_config()
        return total

    def _consume_usage_quota(self, event: AstrMessageEvent, feature_key: str) -> int | None:
        if self._is_quota_exempt(event):
            return None

        user_id = str(event.get_sender_id() or "").strip()
        if not user_id:
            return -1

        current = self._get_usage_quota(user_id, feature_key)
        if current <= 0:
            return -1

        remaining = current - 1
        self._get_user_usage_store(user_id)[feature_key] = remaining
        self._save_config()
        return remaining

    def _save_config(self):
        save_config = getattr(self.config, "save_config", None)
        if callable(save_config):
            save_config()

    def _parse_add_quota_query(self, message: str) -> tuple[str, str, int]:
        args = self._strip_command(message)
        if len(args) != 3 or args[0] in {"help", "帮助", "?"}:
            raise ValueError(self._quota_usage())

        user_id = args[0].strip()
        feature_key = FEATURE_ALIASES.get(args[1].strip().lower()) or FEATURE_ALIASES.get(args[1].strip())
        if feature_key is None:
            raise ValueError("参数错误：未知功能。\n" + self._quota_usage())

        try:
            amount = int(args[2])
        except ValueError as exc:
            raise ValueError("参数错误：次数必须是整数。\n" + self._quota_usage()) from exc
        if amount == 0:
            raise ValueError("参数错误：次数不能为 0。")

        return user_id, feature_key, amount

    def _format_usage_quota(self, user_id: str) -> str:
        user_store = self._get_user_usage_store(user_id)
        lines = [f"用户 {user_id} 使用次数："]
        for feature_key, label in FEATURE_LABELS.items():
            value = user_store.get(feature_key, 0)
            count = value if isinstance(value, int) else 0
            lines.append(f"{label}：{count}")
        return "\n".join(lines)

    def _config_id_set(self, key: str) -> set[str]:
        value = self.config.get(key, [])
        if isinstance(value, list):
            items = value
        elif isinstance(value, str):
            normalized = value.replace("，", ",").replace(";", ",").replace("；", ",")
            items = normalized.replace("\n", ",").replace("\t", ",").split(",")
        else:
            return set()

        return {str(item).strip() for item in items if str(item).strip()}

    def _parse_query(self, message: str) -> dict[str, Any]:
        args = self._strip_command(message)
        if len(args) < 3 or args[0] in {"help", "帮助", "?"}:
            raise ValueError(self._usage())

        try:
            zb = int(args[0])
        except ValueError as exc:
            raise ValueError("参数错误：战备值必须是整数。\n" + self._usage()) from exc

        if not MIN_ZB <= zb <= MAX_ZB:
            raise ValueError(f"参数错误：战备值需在 {MIN_ZB} 到 {MAX_ZB} 之间。")

        mode = MODE_ALIASES.get(args[1].strip().lower()) or MODE_ALIASES.get(args[1].strip())
        if mode is None:
            raise ValueError("参数错误：第三个参数必须是“需求/xq”或“已有/yy”。\n" + self._usage())

        selected_options = self._parse_gear_tokens(args[2:])
        if not selected_options:
            raise ValueError("参数错误：请至少填写一个装备类型。\n" + self._usage())

        params: dict[str, Any] = {"zb": zb, "exchange": 0, "is_gun2_off": 1}
        if mode == "需求":
            params.update({name: int(name in selected_options) for name in GEAR_OPTIONS})
        else:
            params.update({name: int(name not in selected_options) for name in GEAR_OPTIONS})

        if not any(params[name] == 1 for name in GEAR_OPTIONS):
            raise ValueError("参数错误：至少需要启用一个装备类型。")

        return params

    def _strip_command(self, message: str) -> list[str]:
        parts = message.strip().split()
        if not parts:
            return []
        return parts[1:]

    def _parse_gear_tokens(self, tokens: list[str]) -> set[str]:
        selected_options: set[str] = set()
        for token in tokens:
            option = OPTION_ALIASES.get(token.strip().lower()) or OPTION_ALIASES.get(token.strip())
            if option is None:
                raise ValueError(f"参数错误：未知装备类型 {token}。\n" + self._usage())
            selected_options.add(option)
        return selected_options

    def _parse_manufacture_query(self, message: str) -> dict[str, int]:
        args = self._strip_command(message)
        if len(args) != 2 or args[0] in {"help", "帮助", "?"}:
            raise ValueError(self._manufacture_usage())

        manufacture_type = MANUFACTURE_TYPE_ALIASES.get(args[0].strip().lower()) or MANUFACTURE_TYPE_ALIASES.get(args[0].strip())
        if manufacture_type is None:
            raise ValueError("参数错误：部门必须是中文名或拼音首字母缩写。\n" + self._manufacture_usage())

        try:
            level = int(args[1])
        except ValueError as exc:
            raise ValueError("参数错误：部门等级必须是整数。\n" + self._manufacture_usage()) from exc
        if level not in {1, 2, 3}:
            raise ValueError("参数错误：部门等级必须是 1 到 3。\n" + self._manufacture_usage())

        return {"t": manufacture_type, "l": level}

    def _parse_trade_query(self, message: str) -> str:
        args = self._strip_command(message)
        if not args or args[0] in {"help", "帮助", "?"}:
            raise ValueError(self._trade_usage())

        keyword = " ".join(args).strip()
        if len(keyword) < 2:
            raise ValueError("参数错误：物品名称至少需要 2 个字符。\n" + self._trade_usage())
        return keyword

    def _search_trade_items(self, keyword: str) -> list[dict[str, Any]]:
        normalized_keyword = self._normalize_text(keyword)
        max_matches = int(self.config.get("trade_match_limit", 8) or 8)
        max_matches = max(1, min(max_matches, 20))
        scored_items: list[tuple[int, str, dict[str, Any]]] = []

        for item in self.item_info_items:
            name = self._item_name(item)
            if not name:
                continue

            normalized_name = self._normalize_text(name)
            if normalized_keyword not in normalized_name:
                continue

            if normalized_name == normalized_keyword:
                score = 0
            elif normalized_name.startswith(normalized_keyword):
                score = 1
            else:
                score = 2
            scored_items.append((score, name, item))

        scored_items.sort(key=lambda value: (value[0], len(value[1]), value[1]))
        return [item for _, _, item in scored_items[:max_matches]]

    def _format_custom_result(self, payload: dict[str, Any], target_zb: int) -> str:
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("接口 data 字段结构异常")

        items = data.get("data")
        if not isinstance(items, list) or not items:
            raise ValueError("接口未返回可用配装")

        result_limit = int(self.config.get("result_item_limit", 12) or 12)
        result_limit = max(1, min(result_limit, 30))

        lines = [
            "卡战备查询结果",
            f"目标战备：{self._fmt_num(target_zb)}",
            f"方案名称：{data.get('name', '自定义')}",
            f"总战备：{self._fmt_num(data.get('jz'))}",
            f"总价格：{self._fmt_num(data.get('price'))}",
            f"差值：{self._fmt_num(data.get('cz'))}",
            "配装明细：",
        ]

        for item in items[:result_limit]:
            if not isinstance(item, dict):
                continue
            item_type = item.get("type") or "未知类型"
            name = item.get("name") or "未命名"
            price = self._fmt_num(item.get("price"))
            jz = self._fmt_num(item.get("jz"))
            grade = item.get("grade", "-")
            lines.append(f"- {item_type}：{name} | 价格 {price} | 战备 {jz} | 等级 {grade}")

        if len(items) > result_limit:
            lines.append(f"另有 {len(items) - result_limit} 项未展示，可在配置中调高 result_item_limit。")

        return "\n".join(lines)

    def _format_market_result(self, payload: dict[str, Any], lv: int) -> str:
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("接口 data 字段结构异常")

        loadouts = data.get("data")
        if not isinstance(loadouts, list) or not loadouts:
            raise ValueError("接口未返回可用配装")

        scheme_limit = int(self.config.get("result_scheme_limit", 5) or 5)
        scheme_limit = max(1, min(scheme_limit, 10))
        item_limit = int(self.config.get("result_item_limit", 12) or 12)
        item_limit = max(1, min(item_limit, 30))

        lines = [
            "实时卡战备查询结果",
            f"参数：{lv}（{LV_OPTIONS[lv]}）",
            f"更新时间：{data.get('time', '-')}",
        ]

        for index, loadout in enumerate(loadouts[:scheme_limit], 1):
            if not isinstance(loadout, dict):
                continue
            lines.extend(
                [
                    "",
                    f"方案 {index}：{loadout.get('name', '未命名')}",
                    f"总战备：{self._fmt_num(loadout.get('jz'))}",
                    f"总价格：{self._fmt_num(loadout.get('price'))}",
                    f"差值：{self._fmt_num(loadout.get('cz'))}",
                    "配装明细：",
                ]
            )

            items = loadout.get("data")
            if not isinstance(items, list) or not items:
                lines.append("- 无明细")
                continue

            for item in items[:item_limit]:
                if not isinstance(item, dict):
                    continue
                item_type = item.get("type") or "未知类型"
                name = item.get("name") or "未命名"
                price = self._fmt_num(item.get("price"))
                jz = self._fmt_num(item.get("jz"))
                grade = item.get("grade", "-")
                lines.append(f"- {item_type}：{name} | 价格 {price} | 战备 {jz} | 等级 {grade}")

            if len(items) > item_limit:
                lines.append(f"另有 {len(items) - item_limit} 项未展示。")

        if len(loadouts) > scheme_limit:
            lines.append(f"\n另有 {len(loadouts) - scheme_limit} 个方案未展示，可在配置中调高 result_scheme_limit。")

        return "\n".join(lines)

    def _format_trade_items(
        self,
        keyword: str,
        matches: list[dict[str, Any]],
        prices: dict[int, dict[str, Any]],
    ) -> str:
        lines = [
            "交易行物品查询结果",
            f"关键词：{keyword}",
        ]

        for index, item in enumerate(matches, 1):
            item_id = self._trade_price_item_id(item)
            price_item = prices.get(item_id) if item_id is not None else None
            price = self._first_value(price_item, ("price", "avgPrice", "lowPrice", "minPrice")) if price_item else None
            updated_at = self._format_trade_update_time(price_item)
            category = self._first_value(item, ("secondClassCN", "secondClass", "type", "category")) or "未知类型"
            grade = self._first_value(item, ("grade", "rank", "level", "quality")) or "-"
            primary_class = self._first_value(item, ("primaryClassCN", "primaryClass")) or "-"
            status = self._format_trade_price_status(price_item, price)

            lines.append(
                f"{index}. {self._item_name(item)} | {category} | 品质 {grade} | "
                f"分类 {primary_class} | {status}"
            )
            if updated_at:
                lines.append(f"   更新时间：{updated_at}")
            if item_id is not None:
                lines.append(f"   交易行ID：{item_id}")

        return "\n".join(lines)

    def _format_trade_price_status(self, price_item: dict[str, Any] | None, price: Any) -> str:
        if not price_item:
            return "暂无价格"
        if price is None or price == "":
            return "价格 -"
        parts = [f"当前 {self._fmt_num(price)}"]
        price_start = self._first_value(price_item, ("price_start", "startPrice"))
        if price_start is not None:
            parts.append(f"起价 {self._fmt_num(price_start)}")

        day_3_price = self._first_value(price_item, ("day_3_price", "day3Price"))
        day_7_price = self._first_value(price_item, ("day_7_price", "day7Price"))
        day_30_price = self._first_value(price_item, ("day_30_price", "day30Price"))
        if day_3_price is not None:
            parts.append(f"3日 {self._fmt_num(day_3_price)}")
        if day_7_price is not None:
            parts.append(f"7日 {self._fmt_num(day_7_price)}")
        if day_30_price is not None:
            parts.append(f"30日 {self._fmt_num(day_30_price)}")

        ratio = self._first_value(price_item, ("bl", "ratio"))
        if ratio is not None:
            parts.append(f"波动 {ratio}%")
        return " | ".join(parts)

    def _format_trade_update_time(self, price_item: dict[str, Any] | None) -> str | None:
        if not price_item:
            return None

        raw_time = self._first_value(price_item, ("is_get_time", "time", "updated_at", "updateTime"))
        if raw_time is None:
            return None

        if isinstance(raw_time, int):
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(raw_time))
        if isinstance(raw_time, str) and raw_time.strip().isdigit():
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(raw_time.strip())))
        return str(raw_time)

    def _trade_price_item_id(self, item: dict[str, Any]) -> int | None:
        oid = self._first_int(item, ("oid",))
        if oid:
            return oid
        return self._first_int(item, ("id", "objectID"))

    def _format_map_password(self, payload: dict[str, Any]) -> str:
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError("接口 data 字段结构异常")

        lines = ["今日密码"]
        for key, map_name in MAP_NAMES.items():
            passwords = data.get(key)
            if not isinstance(passwords, list):
                raise ValueError(f"接口 {key} 字段结构异常")

            clean_passwords = [str(password).strip() for password in passwords if str(password).strip()]
            display_value = "、".join(clean_passwords) if clean_passwords else "-"
            lines.append(f"{map_name}：{display_value}")

        lines.append("值为 - 表示暂未更新。")
        return "\n".join(lines)

    def _format_manufacture_profit(self, payload: dict[str, Any], manufacture_type: int, level: int) -> str:
        data = payload.get("data")
        if not isinstance(data, list):
            raise ValueError("接口 data 字段结构异常")
        if not data:
            raise ValueError("接口未返回制造利润数据")

        result_limit = int(self.config.get("result_item_limit", 12) or 12)
        result_limit = max(1, min(result_limit, 30))
        items = [item for item in data if isinstance(item, dict)]
        items.sort(key=lambda item: self._safe_int(item.get("price_hour")), reverse=True)

        lines = [
            "制造利润查询结果",
            f"部门：{manufacture_type}（{MANUFACTURE_TYPES[manufacture_type]}）",
            f"等级：{level}",
            "按每小时收益从高到低展示：",
        ]

        for index, item in enumerate(items[:result_limit], 1):
            name = item.get("name") or "未命名"
            category = item.get("secondClassCN") or item.get("secondClass") or "未知类型"
            period = item.get("period", "-")
            price = self._fmt_num(item.get("price"))
            price_hour = self._fmt_num(item.get("price_hour"))
            price_max = self._fmt_num(item.get("priceMax"))
            fee = self._fmt_num(item.get("sxf"))
            unlock_level = item.get("unlockLevel", "-")
            lines.append(
                f"{index}. {name}（{category}）| 到手 {price} | 每小时 {price_hour} | "
                f"未扣费 {price_max} | 手续费 {fee} | 时长 {period}h | 解锁 {unlock_level}级"
            )

        if len(items) > result_limit:
            lines.append(f"另有 {len(items) - result_limit} 项未展示，可在配置中调高 result_item_limit。")

        return "\n".join(lines)

    def _safe_int(self, value: Any) -> int:
        return value if isinstance(value, int) else 0

    def _first_int(self, item: dict[str, Any], keys: tuple[str, ...]) -> int | None:
        for key in keys:
            value = item.get(key)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.strip().isdigit():
                return int(value.strip())
        return None

    def _first_value(self, item: dict[str, Any] | None, keys: tuple[str, ...]) -> Any:
        if not item:
            return None
        for key in keys:
            value = item.get(key)
            if value is not None and value != "":
                return value
        return None

    def _item_name(self, item: dict[str, Any]) -> str:
        value = self._first_value(item, ("objectName", "name", "itemName", "item_name"))
        return str(value).strip() if value is not None else ""

    def _normalize_text(self, value: str) -> str:
        return value.strip().lower().replace(" ", "")

    def _fmt_num(self, value: Any) -> str:
        if isinstance(value, int):
            return f"{value:,}"
        if isinstance(value, str) and value.strip().isdigit():
            return f"{int(value.strip()):,}"
        return str(value) if value is not None else "-"

    def _usage(self) -> str:
        return (
            "用法：/自定义卡战备 <战备值> <需求/已有> <装备类型...>\n"
            "示例：/自定义卡战备 78000 需求 胸挂 背包\n"
            "示例：/zdykzb 16000 xq tk xg\n"
            "示例：/自定义卡战备 78000 已有 胸挂 背包\n"
            "装备类型：背包、枪/枪械、护甲、手枪、头盔、胸挂。兑换固定关闭，双枪固定关闭。"
        )

    def _manufacture_usage(self) -> str:
        return (
            "用法：/制造利润 <部门> <部门等级>\n"
            "示例：/制造利润 技术中心 3\n"
            "示例：/zzlr jszx 3\n"
            "部门：技术中心/jszx，工作台/gzt，制药台/zyt，防具台/fjt。部门等级：1-3。"
        )

    def _quota_usage(self) -> str:
        return (
            "用法：/添加次数 <用户ID> <功能> <次数>\n"
            "示例：/添加次数 123456 kzb 10\n"
            "功能：kzb 卡战备，zdykzb 自定义卡战备，jrmm 今日密码，zzlr 制造利润，jyh 交易行查询。"
        )

    def _trade_usage(self) -> str:
        return (
            "用法：/交易行 <物品名称关键词>\n"
            "示例：/交易行 Vector\n"
            "示例：/jyh DAS"
        )

    def _help_text(self) -> str:
        return (
            "可用命令：\n"
            "1. /三角洲帮助 或 /df_help /df_bz：查看本帮助。\n"
            "2. /卡战备 或 /kzb：查看实时卡战备参数，回复 0-5 查询。\n"
            "3. /自定义卡战备 或 /zdykzb：自定义凑战备。\n"
            "   用法：/自定义卡战备 <战备值> <需求/xq/已有/yy> <装备类型...>\n"
            "   示例：/zdykzb 16000 xq tk xg\n"
            "   装备别名：bb 背包，qx/gun 枪械，hj 护甲，sq 手枪，tk 头盔，xg 胸挂。\n"
            "4. /今日密码 或 /jrmm：查询今日地图密码。"
            "\n5. /制造利润 或 /zzlr：查询特勤处制造利润。\n"
            "   用法：/制造利润 <部门> <部门等级>，示例：/zzlr jszx 3。\n"
            "6. /交易行 或 /jyh：查询交易行物品价格。\n"
            "   用法：/交易行 <物品名称关键词>，示例：/jyh Vector。\n"
            "7. /添加次数 或 /tjcs：管理员为用户添加功能使用次数。\n"
            "   示例：/tjcs 123456 kzb 10。\n"
            "8. /查询次数 或 /cxcs：查询自己剩余次数；管理员可加用户ID查询他人。"
        )

    def _lv_usage(self) -> str:
        lines = ["请选择卡战备数字参数，60 秒内回复数字即可查询："]
        for lv, description in LV_OPTIONS.items():
            lines.append(f"{lv}：{description}")
        lines.append("回复“取消”可退出。")
        return "\n".join(lines)

    def _pending_key(self, event: AstrMessageEvent) -> str:
        return f"{event.unified_msg_origin}:{event.get_sender_id()}"

    async def terminate(self):
        """插件卸载或停用时调用。"""
