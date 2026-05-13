import sys
from pathlib import Path

# Add src to sys.path so we can import from reliability_lab
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from reliability_lab.cache import SharedRedisCache

def main():
    print("=== Redis Cache Interactive Test ===")
    
    # Initialize Redis cache with 5 minute TTL and similarity threshold 0.85
    redis_url = "redis://localhost:6379/0"
    cache = SharedRedisCache(redis_url=redis_url, ttl_seconds=300, similarity_threshold=0.85)
    
    if not cache.ping():
        print(f"[LỖI] Không thể kết nối tới Redis tại {redis_url}.")
        print("Hãy chắc chắn rằng bạn đã chạy 'make docker-up' để khởi động Redis.")
        return
        
    print(f"[OK] Đã kết nối thành công tới Redis ({redis_url}).\n")
    
    print("Nhập chuỗi 1 (sẽ được lưu vào Redis cache):")
    query1 = input("> ").strip()
    
    # Lưu vào cache
    mock_response = f"Cached response từ Redis cho: '{query1}'"
    cache.set(query1, mock_response, {"expected_risk": "policy"})
    print(f"\nĐã lưu vào Redis: '{query1}' -> '{mock_response}'")
    
    print("\nNhập chuỗi 2 (sẽ được dùng để tìm trong Redis cache):")
    query2 = input("> ").strip()
    
    print(f"\nĐang kiểm tra Redis cache cho: '{query2}'...")
    
    # Lấy từ cache
    cached_val, score = cache.get(query2, {"expected_risk": "policy"})
    
    if cached_val:
        print(f"\n[CACHE HIT] Tìm thấy kết quả trong Redis! (Score: {score:.4f})")
        print(f"Kết quả trả về: {cached_val}")
    else:
        print(f"\n[CACHE MISS] Không tìm thấy kết quả phù hợp trong Redis (Score: {score:.4f}).")

if __name__ == "__main__":
    main()
