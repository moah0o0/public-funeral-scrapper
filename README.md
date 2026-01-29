# 부산 공영장례 스크래퍼

부산광역시 16개 구청의 공영장례 정보를 수집하고 텔레그램으로 알림을 전송하는 자동화 시스템입니다.

## 주요 기능

- **16개 구청 스크래핑**: 부산광역시 전 구청의 공영장례 공고 수집
- **GPT-4o 분석**: 비정형 텍스트를 구조화된 데이터로 변환
- **텔레그램 알림**: 구청별 채널 및 통합 채널로 실시간 알림
- **Tor 자동 폴백**: 차단 시 자동으로 Tor 프록시 사용
- **Pocketbase 저장**: 모든 데이터 영구 저장 및 관리
- **웹 모니터링**: 실시간 대시보드 (GitHub Pages)

## 디렉터리 구조

```
public-funeral-scrapper/
├── main.py                 # 엔트리포인트
├── config.py               # 설정 관리
├── requirements.txt        # 의존성
├── Dockerfile              # 컨테이너 빌드
├── docker-compose.yml      # 컨테이너 실행
│
├── core/
│   ├── scheduler.py        # APScheduler 기반 스케줄러
│   ├── pipeline.py         # 3단계 파이프라인 (수집→분석→전송)
│   └── http_client.py      # HTTP 클라이언트 (Tor 폴백)
│
├── scrapers/
│   ├── base.py             # BaseScraper 추상 클래스
│   └── districts/          # 16개 구청별 스크래퍼
│
├── services/
│   ├── pocketbase.py       # Pocketbase 클라이언트
│   ├── telegram.py         # 텔레그램 알림
│   └── gpt_analyzer.py     # GPT-4o 분석
│
├── utils/
│   ├── logger.py           # 로깅
│   └── metrics.py          # 성능 메트릭
│
├── migration/
│   └── json_to_pocketbase.py  # JSON → Pocketbase 마이그레이션
│
└── docs/
    └── index.html          # 모니터링 대시보드 (GitHub Pages)
```

## 설치

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 환경변수 설정

`.env.example`을 복사하여 `.env` 파일 생성:

```bash
cp .env.example .env
```

필수 환경변수:

| 변수명 | 설명 |
|--------|------|
| `TELEGRAM_BOT_TOKEN` | 텔레그램 봇 토큰 |
| `TELEGRAM_ERROR_CHANNEL` | 에러 알림 채널 ID |
| `TELEGRAM_GENERAL_CHANNEL` | 일반 알림 채널 ID |
| `TELEGRAM_FUNERAL_MAIN` | 부고 통합 채널 ID |
| `OPENAI_API_KEY` | OpenAI API 키 |
| `POCKETBASE_URL` | Pocketbase 서버 URL |
| `POCKETBASE_EMAIL` | Pocketbase 관리자 이메일 |
| `POCKETBASE_PASSWORD` | Pocketbase 관리자 비밀번호 |

## 사용법

### 스케줄러 모드 (기본)

15분 간격으로 자동 실행:

```bash
python main.py
```

### 1회 실행

```bash
python main.py --once
```

### RAW 수집 건너뛰기 (분석/전송만)

```bash
python main.py --once --skip-raw
```

### 데이터 정리

중복/고아 레코드 정리:

```bash
python main.py --cleanup
```

### JSON 마이그레이션

기존 JSON 데이터를 Pocketbase로 이전:

```bash
python main.py --migrate
```

## Docker 배포

### 빌드 및 실행

```bash
docker-compose up -d --build
```

### 로그 확인

```bash
docker-compose logs -f
```

### Coolify 배포

1. GitHub 레포지토리 연결
2. Dockerfile 자동 감지
3. 환경변수 설정 (Coolify UI에서)
4. 배포

## 모니터링 대시보드

GitHub Pages로 호스팅: https://public-funeral-monitor.bapc.kr

### 기능

- 실시간 통계 (RAW, 분석완료, 전송완료, 미분석, 미전송)
- 로그 조회 (페이지네이션)
- 부고 목록 (페이지네이션)
- 실행 메트릭 (페이지네이션)
- 구청별 현황

## Pocketbase 컬렉션

| 컬렉션 | 용도 |
|--------|------|
| `funeral_raw` | 원본 스크래핑 데이터 |
| `funeral_analyzed` | GPT 분석 결과 |
| `funeral_sent` | 전송 완료 기록 |
| `scraper_log` | 실행 로그 |
| `scraper_metrics` | 성능 메트릭 |

## 대상 구청

| 구청 | Tor 필요 |
|------|----------|
| 해운대구 | O |
| 금정구 | O |
| 사상구 | O |
| 진구 | O |
| 중구 | O |
| 북구 | - |
| 동구 | - |
| 동래구 | - |
| 강서구 | - |
| 기장군 | - |
| 남구 | - |
| 사하구 | - |
| 서구 | - |
| 수영구 | - |
| 영도구 | - |
| 연제구 | - |

## 라이선스

Private - 부산퀴어행동 (BAPC)
