# DuckDB 로컬 개발 환경 설정

Docker 컨테이너에서 DuckDB를 실행하고, MinIO(S3)와 연동합니다.

## ⚙️ 구성 파일 및 설명
### 1. Dockerfile
- Debian 기반 컨테이너를 사용하여 DuckDB CLI를 설치
- `init.sql.template` 파일을 환경 변수와 함께 `.duckdbrc`로 변환하여 DuckDB 설정을 자동 적용
### 2. docker-compose.yaml
- DuckDB 컨테이너를 `.env` 파일을 기반으로 실행
- DuckDB 데이터베이스를 `/data/database.db`에 저장
- MinIO(S3) 연동을 위해 환경 변수를 설정
### 3. init.sql.template
- 컨테이너 시작 시 DuckDB에서 HTTPFS 확장을 로드하고, MinIO(S3) 연결을 위한 설정을 수행
- `.env` 파일의 변수 값을 `envsubst`를 사용하여 `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`를 적용

## ▶️ 실행하기
### 1. .env 파일 설정
.env 파일을 작성하여 환경 변수를 설정합니다.
```ini
# DuckDB 설정
DUCKDB_PORT=port-number
DUCKDB_DATA_PATH=local-data-path

# 컨테이너 내부에서 사용할 포트
CONTAINER_PORT=port-number

# MinIO(S3) 설정
S3_ENDPOINT=minio-server-ip:minio-api-port
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
```

### 2. 컨테이너 실행
```shell
docker compose up --build
docker ps
```

### 3. DuckDB CLI 접속 및 설정 확인
컨테이너 내부에서 DuckDB 실행:
```shell
docker exec -it duckdb_container duckdb
```

설정 확인:
```sql
SELECT current_setting('s3_endpoint') AS endpoint;
SELECT current_setting('s3_access_key_id') AS access_key;  -- , ...
```

### 4. MinIO(S3)에서 파일 읽기 테스트
```sql
SELECT * FROM read_parquet(
    's3://your-bucket/data/silver/steam/details/*.parquet'
);
```
