# src/twitch/ingest/twitch_settings.py

# https://api.twitch.tv/helix/streams  # 시청자 많은 순으로 현재 생방송 목록 표시 (최대 100개)
# https://api.twitch.tv/helix/games/top  # 시청자 많은 카테고리 게임 표시

# Twitch 요청 제한을 보는 방법 (헤더에 포함돼 있음)
# ratelimit_limit = response.headers.get('Ratelimit-Limit')
# ratelimit_remaining = response.headers.get('Ratelimit-Remaining')
# ratelimit_reset = response.headers.get('Ratelimit-Reset')
# print(f"Ratelimit-Limit: {ratelimit_limit}")
# print(f"Ratelimit-Remaining: {ratelimit_remaining}")
# print(f"Ratelimit-Reset: {ratelimit_reset}")

from common.config import Config

TWC_CLIENT_ID = Config.TWC_CLIENT_ID
TWC_ACCESS_TOKEN = Config.TWC_ACCESS_TOKEN
MINIO_BUCKET_NAME = Config.MINIO_BUCKET_NAME
