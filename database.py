import difflib
import logging
import os
import random
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)


class Database:
    USER_FIELDS = (
        "user_id",
        "username",
        "first_name",
        "last_name",
        "registration_date",
        "is_premium",
        "premium_until",
        "last_rotation_date",
        "total_searches",
        "total_views",
    )
    CHANNEL_FIELDS = (
        "id",
        "channel_id",
        "channel_name",
        "channel_username",
        "channel_type",
        "is_active",
        "added_date",
        "invite_link",
    )
    MOVIE_FIELDS = (
        "id",
        "title",
        "code",
        "file_id",
        "file_type",
        "media_type",
        "category",
        "description",
        "year",
        "rating",
        "views",
        "added_date",
        "is_active",
        "source_chat_id",
        "source_message_id",
    )
    EPISODE_FIELDS = (
        "id",
        "movie_id",
        "episode_number",
        "episode_title",
        "file_id",
        "file_type",
        "added_date",
        "source_chat_id",
        "source_message_id",
    )
    PAYMENT_FIELDS = (
        "id",
        "user_id",
        "amount",
        "payment_type",
        "status",
        "transaction_date",
    )

    USER_DEFAULTS = {
        "user_id": None,
        "username": None,
        "first_name": None,
        "last_name": None,
        "registration_date": None,
        "is_premium": 0,
        "premium_until": None,
        "last_rotation_date": None,
        "total_searches": 0,
        "total_views": 0,
    }
    CHANNEL_DEFAULTS = {
        "id": None,
        "channel_id": None,
        "channel_name": None,
        "channel_username": None,
        "channel_type": None,
        "is_active": 1,
        "added_date": None,
        "invite_link": None,
    }
    MOVIE_DEFAULTS = {
        "id": None,
        "title": None,
        "code": None,
        "file_id": None,
        "file_type": "video",
        "media_type": None,
        "category": None,
        "description": None,
        "year": None,
        "rating": None,
        "views": 0,
        "added_date": None,
        "is_active": 1,
        "source_chat_id": None,
        "source_message_id": None,
    }
    EPISODE_DEFAULTS = {
        "id": None,
        "movie_id": None,
        "episode_number": None,
        "episode_title": None,
        "file_id": None,
        "file_type": "video",
        "added_date": None,
        "source_chat_id": None,
        "source_message_id": None,
    }
    PAYMENT_DEFAULTS = {
        "id": None,
        "user_id": None,
        "amount": 0,
        "payment_type": None,
        "status": "pending",
        "transaction_date": None,
    }

    def __init__(
        self,
        mongo_uri: Optional[str] = None,
        mongo_db_name: Optional[str] = None,
        default_premium_price: int = 5000,
        default_card_number: str = "8600 0000 0000 0000",
        default_card_owner: str = "Ozodbbek Mamatov",
        sqlite_fallback_path: str = "telegram_cinema_bot.db",
    ):
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://mongo:UgITPEgTazcyGdpjSGdeMOFkBvBlvgsg@yamanote.proxy.rlwy.net:42488")
        self.mongo_db_name = mongo_db_name or os.getenv("MONGO_DB_NAME", "telegram_cinema_bot")
        self.sqlite_fallback_path = os.getenv("SQLITE_DB_PATH", sqlite_fallback_path)
        self.default_settings = {
            "premium_price_monthly": str(default_premium_price),
            "card_number": default_card_number,
            "card_owner": default_card_owner,
        }

        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db_name]
        self.init_database()

        if os.getenv("MIGRATE_SQLITE_ON_START", "0") == "1":
            self.migrate_from_sqlite(self.sqlite_fallback_path)

    def init_database(self):
        self.db.users.create_index([("user_id", ASCENDING)], unique=True)
        self.db.channels.create_index([("id", ASCENDING)], unique=True)
        self.db.channels.create_index([("channel_id", ASCENDING)], unique=True)
        self.db.channels.create_index([("channel_type", ASCENDING), ("is_active", ASCENDING)])
        self.db.user_subscriptions.create_index(
            [("user_id", ASCENDING), ("channel_id", ASCENDING), ("rotation_day", ASCENDING)],
            unique=True,
        )
        self.db.user_subscriptions.create_index([("user_id", ASCENDING), ("rotation_day", ASCENDING)])
        self.db.movies.create_index([("id", ASCENDING)], unique=True)
        self.db.movies.create_index([("code", ASCENDING)], unique=True)
        self.db.movies.create_index([("category", ASCENDING), ("is_active", ASCENDING)])
        self.db.movies.create_index([("is_active", ASCENDING), ("views", DESCENDING)])
        self.db.movies.create_index([("source_chat_id", ASCENDING), ("source_message_id", ASCENDING)])
        self.db.series_episodes.create_index([("id", ASCENDING)], unique=True)
        self.db.series_episodes.create_index([("movie_id", ASCENDING), ("episode_number", ASCENDING)], unique=True)
        self.db.series_episodes.create_index([("source_chat_id", ASCENDING), ("source_message_id", ASCENDING)])
        self.db.search_statistics.create_index([("id", ASCENDING)], unique=True)
        self.db.search_statistics.create_index([("search_date", DESCENDING)])
        self.db.search_statistics.create_index([("user_id", ASCENDING), ("search_date", DESCENDING)])
        self.db.view_statistics.create_index([("id", ASCENDING)], unique=True)
        self.db.view_statistics.create_index([("movie_id", ASCENDING), ("view_date", DESCENDING)])
        self.db.payment_transactions.create_index([("id", ASCENDING)], unique=True)
        self.db.payment_transactions.create_index([("status", ASCENDING), ("transaction_date", DESCENDING)])
        self.db.settings.create_index([("key", ASCENDING)], unique=True)

        for key, value in self.default_settings.items():
            self.db.settings.update_one(
                {"key": key},
                {"$setOnInsert": {"value": str(value)}},
                upsert=True,
            )

        logger.info("MongoDB initialized successfully")

    def _next_id(self, counter_name: str) -> int:
        counter = self.db.counters.find_one_and_update(
            {"_id": counter_name},
            {"$inc": {"seq": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return int(counter.get("seq", 1))

    def _set_counter_floor(self, counter_name: str, floor_value: int):
        if floor_value <= 0:
            return
        current = self.db.counters.find_one({"_id": counter_name})
        current_seq = int(current.get("seq", 0)) if current else 0
        if current_seq < floor_value:
            self.db.counters.update_one(
                {"_id": counter_name},
                {"$set": {"seq": int(floor_value)}},
                upsert=True,
            )

    @staticmethod
    def _extract_day(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        text = str(value)
        if "T" in text:
            return text.split("T", 1)[0]
        if " " in text:
            return text.split(" ", 1)[0]
        return text

    @staticmethod
    def _normalize_chat_id(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _normalize_int(value) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _doc_to_tuple(self, doc: Optional[dict], fields: Tuple[str, ...], defaults: Dict) -> Optional[tuple]:
        if not doc:
            return None
        return tuple(doc.get(field, defaults.get(field)) for field in fields)

    def _user_tuple(self, doc: Optional[dict]) -> Optional[tuple]:
        return self._doc_to_tuple(doc, self.USER_FIELDS, self.USER_DEFAULTS)

    def _channel_tuple(self, doc: Optional[dict]) -> Optional[tuple]:
        return self._doc_to_tuple(doc, self.CHANNEL_FIELDS, self.CHANNEL_DEFAULTS)

    def _movie_tuple(self, doc: Optional[dict]) -> Optional[tuple]:
        return self._doc_to_tuple(doc, self.MOVIE_FIELDS, self.MOVIE_DEFAULTS)

    def _episode_tuple(self, doc: Optional[dict]) -> Optional[tuple]:
        return self._doc_to_tuple(doc, self.EPISODE_FIELDS, self.EPISODE_DEFAULTS)

    def _payment_tuple(self, doc: Optional[dict]) -> Optional[tuple]:
        return self._doc_to_tuple(doc, self.PAYMENT_FIELDS, self.PAYMENT_DEFAULTS)

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        row = self.db.settings.find_one({"key": key}, {"value": 1})
        return row.get("value") if row else default

    def set_setting(self, key: str, value: str):
        self.db.settings.update_one({"key": key}, {"$set": {"value": value}}, upsert=True)

    def add_user(self, user_id: int, username: str, first_name: str, last_name: str = None):
        self.db.users.update_one(
            {"user_id": int(user_id)},
            {
                "$setOnInsert": {
                    "user_id": int(user_id),
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "registration_date": datetime.now().isoformat(),
                    "is_premium": 0,
                    "premium_until": None,
                    "last_rotation_date": None,
                    "total_searches": 0,
                    "total_views": 0,
                }
            },
            upsert=True,
        )

    def get_user(self, user_id: int) -> Optional[tuple]:
        doc = self.db.users.find_one({"user_id": int(user_id)})
        return self._user_tuple(doc)

    def is_premium(self, user_id: int) -> bool:
        user = self.get_user(user_id)
        if user and user[5] == 1:
            if user[6]:
                try:
                    premium_until = datetime.fromisoformat(user[6])
                except ValueError:
                    self.remove_premium(user_id)
                    return False
                if premium_until > datetime.now():
                    return True
                self.remove_premium(user_id)
        return False

    def add_premium(self, user_id: int, days: int = 30):
        premium_until = (datetime.now() + timedelta(days=days)).isoformat()
        self.db.users.update_one(
            {"user_id": int(user_id)},
            {"$set": {"is_premium": 1, "premium_until": premium_until}},
        )

    def remove_premium(self, user_id: int):
        self.db.users.update_one(
            {"user_id": int(user_id)},
            {"$set": {"is_premium": 0, "premium_until": None}},
        )

    def create_payment(self, user_id: int, amount: int, payment_type: str = "card") -> int:
        payment_id = self._next_id("payment_transactions")
        self.db.payment_transactions.insert_one(
            {
                "id": payment_id,
                "user_id": int(user_id),
                "amount": int(amount),
                "payment_type": payment_type,
                "status": "pending",
                "transaction_date": datetime.now().isoformat(),
            }
        )
        return payment_id

    def get_payment(self, payment_id: int) -> Optional[tuple]:
        doc = self.db.payment_transactions.find_one({"id": int(payment_id)})
        return self._payment_tuple(doc)

    def update_payment_status(self, payment_id: int, status: str):
        self.db.payment_transactions.update_one({"id": int(payment_id)}, {"$set": {"status": status}})

    def add_channel(
        self,
        channel_id: str,
        channel_name: str,
        channel_username: str,
        channel_type: str,
        invite_link: Optional[str] = None,
    ):
        doc = {
            "id": self._next_id("channels"),
            "channel_id": str(channel_id),
            "channel_name": channel_name,
            "channel_username": channel_username,
            "channel_type": channel_type,
            "is_active": 1,
            "added_date": datetime.now().isoformat(),
            "invite_link": invite_link,
        }
        try:
            self.db.channels.insert_one(doc)
            return True
        except DuplicateKeyError:
            return False

    def get_all_channels(self, active_only: bool = True) -> List[tuple]:
        query = {"is_active": 1} if active_only else {}
        docs = list(self.db.channels.find(query).sort("id", ASCENDING))
        return [self._channel_tuple(doc) for doc in docs]

    def get_channels_by_type(self, channel_type: str, active_only: bool = True) -> List[tuple]:
        query = {"channel_type": channel_type}
        if active_only:
            query["is_active"] = 1
        docs = list(self.db.channels.find(query).sort("id", ASCENDING))
        return [self._channel_tuple(doc) for doc in docs]

    def delete_channel(self, channel_id: str):
        self.db.channels.delete_one({"channel_id": str(channel_id)})

    def get_daily_channels(self, user_id: int) -> List[tuple]:
        today = datetime.now().date().isoformat()
        channels = self.get_user_today_channels(user_id, today)
        active_count = self.db.channels.count_documents({"is_active": 1})
        target_count = min(6, active_count)
        if channels and len(channels) >= target_count:
            return channels
        return self.rotate_channels(user_id, today)

    def _pick_daily_channels(self, user_id: int, limit: int = 6) -> List[dict]:
        active_channels = list(self.db.channels.find({"is_active": 1}))
        if not active_channels:
            return []

        limit = max(1, min(int(limit), len(active_channels)))
        cutoff = (datetime.now().date() - timedelta(days=7)).isoformat()
        recent_subs = self.db.user_subscriptions.find(
            {"user_id": int(user_id), "rotation_day": {"$gte": cutoff}},
            {"channel_id": 1},
        )
        used_ids = {str(s.get("channel_id")) for s in recent_subs if s.get("channel_id") is not None}

        z_candidates = [
            ch
            for ch in active_channels
            if ch.get("channel_type") == "zayafka" and str(ch.get("channel_id")) not in used_ids
        ]
        if not z_candidates:
            z_candidates = [ch for ch in active_channels if ch.get("channel_type") == "zayafka"]
        p_candidates = [ch for ch in active_channels if ch.get("channel_type") == "public"]

        random.shuffle(z_candidates)
        random.shuffle(p_candidates)

        selected: List[dict] = []
        selected_ids = set()

        def add_unique(source: List[dict], max_take: int):
            for ch in source:
                if len(selected) >= max_take:
                    break
                cid = str(ch.get("channel_id"))
                if cid in selected_ids:
                    continue
                selected.append(ch)
                selected_ids.add(cid)

        add_unique(z_candidates, min(4, limit))
        add_unique(p_candidates, min(limit, len(selected) + 2))

        if len(selected) < limit:
            rest = [ch for ch in active_channels if str(ch.get("channel_id")) not in selected_ids]
            random.shuffle(rest)
            add_unique(rest, limit)

        return selected

    def rotate_channels(self, user_id: int, today: str) -> List[tuple]:
        selected = self._pick_daily_channels(user_id=user_id, limit=6)
        now_iso = datetime.now().isoformat()

        self.db.user_subscriptions.delete_many({"user_id": int(user_id), "rotation_day": today})
        for ch in selected:
            channel_id = str(ch.get("channel_id"))
            self.db.user_subscriptions.update_one(
                {"user_id": int(user_id), "channel_id": channel_id, "rotation_day": today},
                {
                    "$set": {"rotation_date": now_iso},
                    "$setOnInsert": {
                        "user_id": int(user_id),
                        "channel_id": channel_id,
                        "rotation_day": today,
                        "subscribed_date": None,
                    },
                },
                upsert=True,
            )

        self.db.users.update_one(
            {"user_id": int(user_id)},
            {"$set": {"last_rotation_date": now_iso}},
        )
        return [self._channel_tuple(doc) for doc in selected]

    def get_user_today_channels(self, user_id: int, today: str) -> List[tuple]:
        sub_docs = list(
            self.db.user_subscriptions.find(
                {"user_id": int(user_id)},
                {"channel_id": 1, "rotation_day": 1, "rotation_date": 1},
            )
        )
        channel_ids = []
        for sub in sub_docs:
            day = sub.get("rotation_day") or self._extract_day(sub.get("rotation_date"))
            if day == today and sub.get("channel_id") is not None:
                channel_ids.append(str(sub.get("channel_id")))

        if not channel_ids:
            return []

        channel_docs = list(self.db.channels.find({"channel_id": {"$in": channel_ids}, "is_active": 1}))
        channel_map = {str(doc.get("channel_id")): doc for doc in channel_docs}
        ordered = [channel_map[cid] for cid in channel_ids if cid in channel_map]
        return [self._channel_tuple(doc) for doc in ordered]

    def mark_subscription(self, user_id: int, channel_id: str):
        now_iso = datetime.now().isoformat()
        day = self._extract_day(now_iso)
        self.db.user_subscriptions.update_one(
            {"user_id": int(user_id), "channel_id": str(channel_id), "rotation_day": day},
            {
                "$set": {"subscribed_date": now_iso, "rotation_date": now_iso},
                "$setOnInsert": {
                    "user_id": int(user_id),
                    "channel_id": str(channel_id),
                    "rotation_day": day,
                },
            },
            upsert=True,
        )

    def add_movie(
        self,
        title: str,
        code: str,
        file_id: str,
        media_type: str,
        category: str,
        description: str = None,
        year: int = None,
        rating: float = None,
        file_type: str = "video",
        source_chat_id: Optional[str] = None,
        source_message_id: Optional[int] = None,
    ) -> Optional[int]:
        movie_id = self._next_id("movies")
        doc = {
            "id": movie_id,
            "title": title,
            "code": code,
            "file_id": file_id,
            "file_type": file_type,
            "media_type": media_type,
            "category": category,
            "description": description,
            "year": year,
            "rating": rating,
            "views": 0,
            "added_date": datetime.now().isoformat(),
            "is_active": 1,
            "source_chat_id": self._normalize_chat_id(source_chat_id),
            "source_message_id": self._normalize_int(source_message_id),
        }
        try:
            self.db.movies.insert_one(doc)
            return movie_id
        except DuplicateKeyError:
            return None

    def search_movie(self, query: str) -> Optional[tuple]:
        doc = self.db.movies.find_one({"code": query, "is_active": 1})
        if not doc:
            doc = self.db.movies.find_one(
                {"title": {"$regex": re.escape(query), "$options": "i"}, "is_active": 1}
            )
        return self._movie_tuple(doc)

    def search_movies(self, query: str, limit: int = 6) -> List[tuple]:
        q = query.strip()
        if not q:
            return []

        doc = self.db.movies.find_one({"code": q.upper(), "is_active": 1})
        if doc:
            return [self._movie_tuple(doc)]

        exact_docs = list(
            self.db.movies.find(
                {"title": {"$regex": f"^{re.escape(q)}$", "$options": "i"}, "is_active": 1}
            ).limit(limit)
        )
        if exact_docs:
            return [self._movie_tuple(doc) for doc in exact_docs]

        docs = list(
            self.db.movies.find(
                {"title": {"$regex": re.escape(q), "$options": "i"}, "is_active": 1}
            )
            .sort("views", DESCENDING)
            .limit(limit)
        )
        return [self._movie_tuple(doc) for doc in docs]

    def search_movies_fuzzy(self, query: str, limit: int = 6, min_score: float = 0.45) -> List[tuple]:
        q = query.strip().lower()
        if not q:
            return []
        tokens = re.findall(r"[a-z0-9]+", q)
        if not tokens:
            return []

        def norm_text(text: str) -> str:
            text = text.lower()
            text = re.sub(r"[^a-z0-9]+", "", text)
            text = re.sub(r"(.)\1{2,}", r"\1\1", text)
            return text

        q_norm = norm_text(q)
        if not q_norm:
            return []

        token_filters = [{"title": {"$regex": re.escape(t), "$options": "i"}} for t in tokens]
        candidates = list(
            self.db.movies.find({"is_active": 1, "$or": token_filters}).limit(max(limit * 10, 10))
        )

        if not candidates:
            candidates = list(self.db.movies.find({"is_active": 1}).sort("views", DESCENDING).limit(200))

        scored = []
        for movie in candidates:
            title_norm = norm_text(movie.get("title") or "")
            if not title_norm:
                continue
            score = difflib.SequenceMatcher(None, q_norm, title_norm).ratio()
            if score >= min_score:
                scored.append((score, movie))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [self._movie_tuple(doc) for _, doc in scored[:limit]]

    def get_movie_by_source(self, source_chat_id: str, source_message_id: int) -> Optional[tuple]:
        doc = self.db.movies.find_one(
            {
                "source_chat_id": self._normalize_chat_id(source_chat_id),
                "source_message_id": self._normalize_int(source_message_id),
            }
        )
        return self._movie_tuple(doc)

    def get_episode_by_source(self, source_chat_id: str, source_message_id: int) -> Optional[tuple]:
        doc = self.db.series_episodes.find_one(
            {
                "source_chat_id": self._normalize_chat_id(source_chat_id),
                "source_message_id": self._normalize_int(source_message_id),
            }
        )
        return self._episode_tuple(doc)

    def is_code_exists(self, code: str) -> bool:
        return self.db.movies.find_one({"code": code}, {"_id": 1}) is not None

    def find_series_by_title(self, title: str) -> Optional[tuple]:
        doc = self.db.movies.find_one(
            {
                "title": {"$regex": f"^{re.escape(title.strip())}$", "$options": "i"},
                "media_type": "series",
                "is_active": 1,
            }
        )
        return self._movie_tuple(doc)

    def is_channel_registered(self, channel_id: str) -> bool:
        return self.db.channels.find_one({"channel_id": str(channel_id), "is_active": 1}, {"_id": 1}) is not None

    def increment_movie_views(self, movie_id: int):
        self.db.movies.update_one({"id": int(movie_id)}, {"$inc": {"views": 1}})

    def get_similar_movies(self, movie_id: int, category: str, limit: int = 5) -> List[tuple]:
        docs = list(
            self.db.movies.find({"category": category, "id": {"$ne": int(movie_id)}, "is_active": 1})
        )
        random.shuffle(docs)
        return [self._movie_tuple(doc) for doc in docs[:limit]]

    def get_movies_by_category(self, category: str, limit: int = 20) -> List[tuple]:
        docs = list(
            self.db.movies.find({"category": category, "is_active": 1})
            .sort("views", DESCENDING)
            .limit(limit)
        )
        return [self._movie_tuple(doc) for doc in docs]

    def get_trending_movies(self, days: int = 7, limit: int = 10) -> List[tuple]:
        since_date = (datetime.now() - timedelta(days=days)).isoformat()
        grouped = list(
            self.db.view_statistics.aggregate(
                [
                    {"$match": {"view_date": {"$gte": since_date}}},
                    {"$group": {"_id": "$movie_id", "recent_views": {"$sum": 1}}},
                    {"$sort": {"recent_views": -1}},
                    {"$limit": max(limit * 5, 50)},
                ]
            )
        )
        if not grouped:
            return []

        movie_ids = [int(row["_id"]) for row in grouped if row.get("_id") is not None]
        if not movie_ids:
            return []

        docs = list(self.db.movies.find({"id": {"$in": movie_ids}, "is_active": 1}))
        movie_map = {int(doc["id"]): doc for doc in docs if doc.get("id") is not None}

        results = []
        for row in grouped:
            movie_id = row.get("_id")
            if movie_id is None:
                continue
            movie_doc = movie_map.get(int(movie_id))
            if not movie_doc:
                continue
            results.append(self._movie_tuple(movie_doc) + (int(row.get("recent_views", 0) or 0),))
            if len(results) >= limit:
                break
        return results

    def add_series_episode(
        self,
        movie_id: int,
        episode_number: int,
        episode_title: str,
        file_id: str,
        file_type: str = "video",
        source_chat_id: Optional[str] = None,
        source_message_id: Optional[int] = None,
    ) -> bool:
        doc = {
            "id": self._next_id("series_episodes"),
            "movie_id": int(movie_id),
            "episode_number": int(episode_number),
            "episode_title": episode_title,
            "file_id": file_id,
            "file_type": file_type,
            "added_date": datetime.now().isoformat(),
            "source_chat_id": self._normalize_chat_id(source_chat_id),
            "source_message_id": self._normalize_int(source_message_id),
        }
        try:
            self.db.series_episodes.insert_one(doc)
            return True
        except DuplicateKeyError:
            return False

    def get_series_episodes(self, movie_id: int) -> List[tuple]:
        docs = list(self.db.series_episodes.find({"movie_id": int(movie_id)}).sort("episode_number", ASCENDING))
        return [self._episode_tuple(doc) for doc in docs]

    def get_episode(self, movie_id: int, episode_number: int) -> Optional[tuple]:
        doc = self.db.series_episodes.find_one(
            {"movie_id": int(movie_id), "episode_number": int(episode_number)}
        )
        return self._episode_tuple(doc)

    def add_search_stat(self, user_id: int, query: str, found: bool):
        stat_id = self._next_id("search_statistics")
        self.db.search_statistics.insert_one(
            {
                "id": stat_id,
                "user_id": int(user_id),
                "query": query,
                "found": 1 if found else 0,
                "search_date": datetime.now().isoformat(),
            }
        )
        self.db.users.update_one({"user_id": int(user_id)}, {"$inc": {"total_searches": 1}})

    def add_view_stat(self, user_id: int, movie_id: int):
        stat_id = self._next_id("view_statistics")
        self.db.view_statistics.insert_one(
            {
                "id": stat_id,
                "user_id": int(user_id),
                "movie_id": int(movie_id),
                "view_date": datetime.now().isoformat(),
            }
        )
        self.db.users.update_one({"user_id": int(user_id)}, {"$inc": {"total_views": 1}})

    def get_statistics(self) -> Dict:
        stats = {}
        stats["total_users"] = self.db.users.count_documents({})
        stats["premium_users"] = self.db.users.count_documents({"is_premium": 1})
        stats["total_movies"] = self.db.movies.count_documents({"is_active": 1})
        stats["total_series"] = self.db.movies.count_documents({"media_type": "series", "is_active": 1})

        today = datetime.now().date().isoformat()
        tomorrow = (datetime.now().date() + timedelta(days=1)).isoformat()
        active_user_ids = self.db.search_statistics.distinct(
            "user_id", {"search_date": {"$gte": today, "$lt": tomorrow}}
        )
        stats["today_active"] = len(active_user_ids)
        stats["total_searches"] = self.db.search_statistics.count_documents({})
        stats["total_views"] = self.db.view_statistics.count_documents({})
        stats["total_channels"] = self.db.channels.count_documents({"is_active": 1})
        return stats

    def get_top_searches(self, limit: int = 10) -> List[tuple]:
        since_date = (datetime.now() - timedelta(days=7)).date().isoformat()
        rows = list(
            self.db.search_statistics.aggregate(
                [
                    {"$match": {"search_date": {"$gte": since_date}}},
                    {"$group": {"_id": "$query", "search_count": {"$sum": 1}}},
                    {"$sort": {"search_count": -1}},
                    {"$limit": int(limit)},
                ]
            )
        )
        return [(row.get("_id"), int(row.get("search_count", 0))) for row in rows]

    def get_movie_by_id(self, movie_id: int) -> Optional[tuple]:
        doc = self.db.movies.find_one({"id": int(movie_id)})
        return self._movie_tuple(doc)

    def get_movie_title(self, movie_id: int) -> Optional[str]:
        doc = self.db.movies.find_one({"id": int(movie_id)}, {"title": 1})
        return doc.get("title") if doc else None

    def get_movie_title_and_code(self, movie_id: int) -> Optional[Tuple[str, str]]:
        doc = self.db.movies.find_one({"id": int(movie_id)}, {"title": 1, "code": 1})
        if not doc:
            return None
        return doc.get("title"), doc.get("code")

    def get_all_user_ids(self) -> List[int]:
        docs = self.db.users.find({}, {"user_id": 1}).sort("user_id", ASCENDING)
        return [int(doc["user_id"]) for doc in docs if doc.get("user_id") is not None]

    def migrate_from_sqlite(self, sqlite_path: str) -> bool:
        if not sqlite_path or not os.path.exists(sqlite_path):
            logger.warning("SQLite file not found for migration: %s", sqlite_path)
            return False

        force = os.getenv("MIGRATE_SQLITE_FORCE", "0") == "1"
        if not force:
            has_existing = (
                self.db.users.count_documents({}) > 0
                or self.db.channels.count_documents({}) > 0
                or self.db.movies.count_documents({}) > 0
            )
            if has_existing:
                logger.info("MongoDB already has data, sqlite migration skipped")
                return False

        conn = sqlite3.connect(sqlite_path)
        cur = conn.cursor()

        migrated = {
            "users": 0,
            "channels": 0,
            "user_subscriptions": 0,
            "movies": 0,
            "series_episodes": 0,
            "search_statistics": 0,
            "view_statistics": 0,
            "payment_transactions": 0,
            "settings": 0,
        }

        try:
            table_names = {
                row[0]
                for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }

            if "users" in table_names:
                for row in cur.execute("SELECT * FROM users").fetchall():
                    if len(row) < 10:
                        continue
                    doc = {
                        "user_id": int(row[0]),
                        "username": row[1],
                        "first_name": row[2],
                        "last_name": row[3],
                        "registration_date": row[4],
                        "is_premium": int(row[5] or 0),
                        "premium_until": row[6],
                        "last_rotation_date": row[7],
                        "total_searches": int(row[8] or 0),
                        "total_views": int(row[9] or 0),
                    }
                    self.db.users.update_one({"user_id": doc["user_id"]}, {"$set": doc}, upsert=True)
                    migrated["users"] += 1

            if "channels" in table_names:
                for row in cur.execute("SELECT * FROM channels").fetchall():
                    if len(row) < 7:
                        continue
                    doc = {
                        "id": int(row[0]),
                        "channel_id": str(row[1]),
                        "channel_name": row[2],
                        "channel_username": row[3],
                        "channel_type": row[4],
                        "is_active": int(row[5] or 0),
                        "added_date": row[6],
                        "invite_link": row[7] if len(row) > 7 else None,
                    }
                    self.db.channels.update_one({"channel_id": doc["channel_id"]}, {"$set": doc}, upsert=True)
                    migrated["channels"] += 1

            if "user_subscriptions" in table_names:
                for row in cur.execute("SELECT * FROM user_subscriptions").fetchall():
                    if len(row) < 5:
                        continue
                    day = self._extract_day(row[4])
                    doc = {
                        "user_id": int(row[1]),
                        "channel_id": str(row[2]),
                        "subscribed_date": row[3],
                        "rotation_date": row[4],
                        "rotation_day": day,
                    }
                    self.db.user_subscriptions.update_one(
                        {"user_id": doc["user_id"], "channel_id": doc["channel_id"], "rotation_day": day},
                        {"$set": doc},
                        upsert=True,
                    )
                    migrated["user_subscriptions"] += 1

            if "movies" in table_names:
                for row in cur.execute("SELECT * FROM movies").fetchall():
                    if len(row) < 13:
                        continue
                    doc = {
                        "id": int(row[0]),
                        "title": row[1],
                        "code": row[2],
                        "file_id": row[3],
                        "file_type": row[4],
                        "media_type": row[5],
                        "category": row[6],
                        "description": row[7],
                        "year": row[8],
                        "rating": row[9],
                        "views": int(row[10] or 0),
                        "added_date": row[11],
                        "is_active": int(row[12] or 0),
                        "source_chat_id": self._normalize_chat_id(row[13]) if len(row) > 13 else None,
                        "source_message_id": self._normalize_int(row[14]) if len(row) > 14 else None,
                    }
                    self.db.movies.update_one({"code": doc["code"]}, {"$set": doc}, upsert=True)
                    migrated["movies"] += 1

            if "series_episodes" in table_names:
                for row in cur.execute("SELECT * FROM series_episodes").fetchall():
                    if len(row) < 7:
                        continue
                    doc = {
                        "id": int(row[0]),
                        "movie_id": int(row[1]),
                        "episode_number": int(row[2]),
                        "episode_title": row[3],
                        "file_id": row[4],
                        "file_type": row[5],
                        "added_date": row[6],
                        "source_chat_id": self._normalize_chat_id(row[7]) if len(row) > 7 else None,
                        "source_message_id": self._normalize_int(row[8]) if len(row) > 8 else None,
                    }
                    self.db.series_episodes.update_one(
                        {"movie_id": doc["movie_id"], "episode_number": doc["episode_number"]},
                        {"$set": doc},
                        upsert=True,
                    )
                    migrated["series_episodes"] += 1

            if "search_statistics" in table_names:
                for row in cur.execute("SELECT * FROM search_statistics").fetchall():
                    if len(row) < 5:
                        continue
                    doc = {
                        "id": int(row[0]),
                        "user_id": int(row[1]),
                        "query": row[2],
                        "found": int(row[3] or 0),
                        "search_date": row[4],
                    }
                    self.db.search_statistics.update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
                    migrated["search_statistics"] += 1

            if "view_statistics" in table_names:
                for row in cur.execute("SELECT * FROM view_statistics").fetchall():
                    if len(row) < 4:
                        continue
                    doc = {
                        "id": int(row[0]),
                        "user_id": int(row[1]),
                        "movie_id": int(row[2]),
                        "view_date": row[3],
                    }
                    self.db.view_statistics.update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
                    migrated["view_statistics"] += 1

            if "payment_transactions" in table_names:
                for row in cur.execute("SELECT * FROM payment_transactions").fetchall():
                    if len(row) < 6:
                        continue
                    doc = {
                        "id": int(row[0]),
                        "user_id": int(row[1]),
                        "amount": int(row[2]),
                        "payment_type": row[3],
                        "status": row[4],
                        "transaction_date": row[5],
                    }
                    self.db.payment_transactions.update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
                    migrated["payment_transactions"] += 1

            if "settings" in table_names:
                for row in cur.execute("SELECT * FROM settings").fetchall():
                    if len(row) < 2:
                        continue
                    self.db.settings.update_one(
                        {"key": row[0]},
                        {"$set": {"value": row[1]}},
                        upsert=True,
                    )
                    migrated["settings"] += 1

            for counter_name, collection_name in [
                ("channels", "channels"),
                ("movies", "movies"),
                ("series_episodes", "series_episodes"),
                ("payment_transactions", "payment_transactions"),
                ("search_statistics", "search_statistics"),
                ("view_statistics", "view_statistics"),
            ]:
                max_doc = self.db[collection_name].find_one(sort=[("id", DESCENDING)])
                if max_doc and max_doc.get("id") is not None:
                    self._set_counter_floor(counter_name, int(max_doc["id"]))

            logger.info("SQLite -> MongoDB migration completed: %s", migrated)
            return True
        except Exception as exc:
            logger.error("SQLite migration failed: %s", exc)
            return False
        finally:
            conn.close()

    def close(self):
        self.client.close()
