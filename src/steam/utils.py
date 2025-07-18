# [PATH] src/steam/utils.py

def chunk_list(lst, chunk_size):
    """
    리스트를 chunk 단위로 분할
    """
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i+chunk_size]

def standardize_appid_entry(appid):
    """
    appid dict/int 혼용 대응
    (appid, name) 튜플 반환
    """
    if isinstance(appid, dict):
        return appid["appid"], appid.get("name")
    else:
        return appid, None

def ensure_json_serializable(obj):
    """dict의 모든 key를 str로 재귀 변환 (중첩까지!)"""
    if isinstance(obj, dict):
        return {str(k): ensure_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [ensure_json_serializable(v) for v in obj]
    else:
        return obj
