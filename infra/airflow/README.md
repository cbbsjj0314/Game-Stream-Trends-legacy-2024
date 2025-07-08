# Apache Airflow 로컬 개발 환경 설정

이 문서는 Docker를 사용하여 Apache Airflow를 로컬 환경에서 설정하는 방법을 설명합니다.

## ⚙️ 구성 파일 및 설명

### 1. `docker-compose.yaml`
#### Airflow 서비스:
  - Airflow 구성 요소 포함 (`airflow-webserver`, `airflow-scheduler`, `airflow-worker`, `airflow-triggerer`, `airflow-init`)
  - CeleryExecutor 기반의 분산 실행 환경 설정
#### PostgreSQL:
  - Airflow 메타데이터 DB로 사용
  - 데이터 지속성을 위해 `postgres-db-volume`을 사용
#### Redis:
  - Airflow의 메시지 브로커 역할 수행
#### 볼륨 마운트:
  - `dags`, `scripts`, `logs`, `plugins`, `config` 디렉토리를 컨테이너 내부로 마운트

### 2. `Dockerfile`
  - `apache/airflow:2.10.4` 이미지를 기반으로 커스텀 패키지를 추가
  - 필요 패키지(`libpq-dev`, `gcc`, `git`) 설치
  - `poetry`를 사용하여 Python 패키지 의존성 관리

### 3. `.env`
  - 환경 변수 파일

---

## ▶️ 실행하기

### 1. 환경 변수 설정

`.env` 파일을 작성하거나 수정하여 Airflow 설정에 필요한 값을 정의

#### 환경 변수

`.env` 파일에서 설정한 주요 변수는 다음과 같음

| 변수명                     | 설명                                       |
|----------------------------|--------------------------------------------|
| `POSTGRES_USER`           | PostgreSQL 사용자 (기본값: `airflow`)      |
| `POSTGRES_PASSWORD`       | PostgreSQL 비밀번호 (기본값: `airflow`)    |
| `POSTGRES_DB`            | PostgreSQL 데이터베이스 이름 (기본값: `airflow`) |
| `AIRFLOW__CORE__FERNET_KEY` | 보안 키 (필요 시 설정)      |
| `AIRFLOW_ADMIN_USER`      | Airflow 관리자 계정 (기본값: `admin`)      |
| `AIRFLOW_ADMIN_PASSWORD`  | Airflow 관리자 비밀번호 (기본값: `admin`)  |
| `AIRFLOW_ADMIN_EMAIL`     | Airflow 관리자 이메일 (필요 시 설정)          |
| `AIRFLOW_MIN_MEMORY_MB`   | Docker 실행을 위한 최소 메모리 요구량 (기본값: `4000MB`) |
| `AIRFLOW_MIN_CPUS`        | Docker 실행을 위한 최소 CPU 요구량 (기본값: `2`) |
| `AIRFLOW_MIN_DISK_MB`     | Docker 실행을 위한 최소 디스크 공간 요구량 (기본값: `10485760MB`) |

### 2. 컨테이너 실행

다음 명령으로 모든 컨테이너 실행
```sh
docker compose up --build -d
```

최초 실행 시 데이터베이스 초기화를 위해 다음 명령 실행
```sh
docker compose run --rm airflow-init
```

---

## ✅ 확인 및 테스트

### 1. Airflow 구성 확인

- [x] 컨테이너 상태 확인
```
docker ps
```

- [x] Airflow 웹 UI 접속 확인
  - `http://localhost:8080` 접속
  - `.env`의 `AIRFLOW_ADMIN_USER`와 `AIRFLOW_ADMIN_PASSWORD`로 로그인

- [x] Flower 웹 UI 접속 확인
  - `http://localhost:5555` 접속
  - Celery 작업 큐(Task Queue) 상태 확인
  - 실행 중인 작업(worker), 대기 중인 작업(task), 완료된 작업 등을 볼 수 있음

---

## ❌ 문제 해결
1. `max depth exceeded` 에러 발생

`docker compose up --build -d` 명령 실행 시 발생할 수 있음
```
# 1. airflow 관련 볼륨 삭제
docker volume rm $(docker volume ls -q | grep airflow)

# 2. 다시 Docker Compose 실행
docker compose up --build -d
```
