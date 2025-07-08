# MinIO 로컬 개발 환경 설정

이 문서는 MinIO 클러스터를 Docker 및 NGINX 리버스 프록시를 사용하여 로컬 환경에서 설정하는 방법을 설명합니다.

## ⚙️ 구성 파일 및 설명

### 1. `docker-compose.yaml`
#### MinIO 서비스:
  - minio1 ~ 4로 총 4개의 MinIO 노드가 클러스터를 구성
  - 각각의 데이터 디렉토리는 로컬 호스트 경로를 컨테이너 내부 경로로 마운트
#### NGINX:
  - MinIO API 및 콘솔에 대한 리버스 프록시 역할 수행
  - `nginx.conf.template`에서 환경 변수로 포트 및 서버를 동적으로 설정

### 2. `nginx.conf.template`
  - API 및 콘솔 트래픽을 각 MinIO 노드로 분산시킴
  - `envsubst` 명령을 통해 `.env` 파일 변수 설정

### 3. `.env`
  - 환경 변수 파일

---

## ▶️ 실행하기

### 1. 환경 변수 설정

`.env` 파일을 작성하거나 수정하여 MinIO 설정에 필요한 값을 정의

#### 환경 변수

`.env` 파일에서 설정한 주요 변수는 다음과 같음

| 변수명                 | 설명
|------------------------|----------------------------------------
| `MINIO_SERVER_IP`      | MinIO 서버 IP (예: localhost)
| `MINIO_ROOT_USER`      | MinIO 관리자 계정 이름
| `MINIO_ROOT_PASSWORD`  | MinIO 관리자 계정 비밀번호
| `MINIO_ACCESS_KEY`      | MinIO 액세스 키
| `MINIO_SECRET_KEY`  | MinIO 시크릿 키
| `MINIO_API_PORT`       | MinIO API 접근 포트
| `MINIO_CONSOLE_PORT`   | MinIO 콘솔 접근 포트
| `MINIO_NODES`          | MinIO 클러스터 노드 설정
| `MINIO1_DATA1_PATH`    | MinIO1의 첫 번째 데이터 디렉토리 경로

### 2. 컨테이너 실행

다음 명령으로 모든 컨테이너 실행

```
docker compose up --build -d
```

---

## ✅ 확인 및 테스트

### 1. MinIO 구성 확인

- [x] 컨테이너 상태 확인
```
docker ps
```

- [x] MinIO 웹 콘솔 접속 확인
- **호스트 머신**
    - `.env`의 `MINIO_CONSOLE_PORT`로 `http://localhost:${MINIO_CONSOLE_PORT}` 접속
    - `.env`의 `MINIO_ROOT_USER`와 `MINIO_ROOT_PASSWORD`로 로그인
- **클라이언트 머신**
    - `.env`의 `MINIO_SERVER_IP`와 `MINIO_CONSOLE_PORT`로 `http://${MINIO_SERVER_IP}:${MINIO_CONSOLE_PORT}` 접속
    - `.env`의 `MINIO_ROOT_USER`와 `MINIO_ROOT_PASSWORD`로 로그인

### 2. 테스트 스크립트 실행
- **호스트 머신**
    - `dev/minio/scripts/`의 `host_minio_test.py` 실행
    - MinIO에 `host-test-bucket` 버킷 및 테스트 파일 생성돼 있으면 성공
- **클라이언트 머신**
    - `dev/minio/scripts/`의 `client_minio_test.py` 실행
    - MinIO에 `client-test-bucket` 버킷 및 테스트 파일 생성돼 있으면 성공
