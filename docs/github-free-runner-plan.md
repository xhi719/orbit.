# 공개 무료 worker 저장소 검토 계획

## 2026-10-13 공지 기간 및 게임별 캘린더 연결

승인된 기존 파일 범위 안에서 공식 공지의 게시 시각, 완전한 기간과 시간대를 자동 정규화한다. 제목별 섹션은 별도 이벤트로 저장하며 원문 공지와 연결한다. 본문에 명시된 최고등급과 신규/복각 근거가 모두 있을 때만 캐릭터별 공지를 분리한다. 등급, 연도, 서버 지역과 이미지 재사용 권한은 추측하지 않는다.

소녀전선2 HAOPLAY Steam 앱 3308670의 공개 publisher API와 2026-10-07 게시된 10/8 안내를 검토했다. 기간에는 GMT+0이 있지만 한국·Asia·일본·글로벌 서버를 특정할 근거가 없어 출처를 `UNSPECIFIED`로 유지한다. 명시된 원문 기간은 `candidateStartsAt`/`candidateEndsAt`에 저장하고, 확정 일정 필드는 NULL 및 검증 상태 pending을 유지한다. 해당 후보로 예약 픽업·푸시를 생성하지 않는다. 웹 캘린더에서는 서버 확인 필요 기간으로 구분한다. 최고등급 신규/복각과 공지 이미지 승인 수는 확인 전까지 0이다.

공용 데이터만 5분 단위로 화면에서 갱신하며 개인 계정·게임 등록은 변경하지 않는다. 이 화면 갱신은 기존 GitHub 서버 예약 수집과 별개다. 15분 목표 cron과 실행 지연, 출처별 임대 잠금·백오프, Spark 비용/할당량 제한을 유지한다. 모든 게임이 이 출처를 사용하는 것은 아니며 출처 없는 게임의 자동 수집 완료를 선언하지 않는다. 실제 배포·원격 수집·예약 실행은 해당 작업의 실행 보고서로 구분해 검증한다.

현재 변경 역시 payload 19개 및 manifest 1개 범위를 유지한다. 앱 화면·캐릭터/게임 이미지·네이티브 바이너리·키·기기 기록을 공개 저장소에 추가하지 않는다. 추가 클라우드 권한이나 결제 서비스를 생성하지 않는다.

## 2026-10-12 네이티브 출석 연동 변경

아래 초기 계획과 과거 검증 기록은 이력을 위해 보존합니다. 현재는 사용자 승인으로 기존 저장소가 공개되고 Spark 전용 worker와 무료 FCM 발송 권한이 연결되었습니다. 웹 테스트 푸시는 제공자의 수락과 사용자의 실제 수신 확인을 각각 기록했습니다. 네이티브 앱의 실제 게임 실행과 기기 간 출석 동기화는 별도로 검증합니다.

이번 변경은 승인된 기존 19개 payload 파일 안에서만 이루어집니다. `admin.py`, `store.py`, `collector.py`, `notifications.py`, 정책 테스트와 이 문서를 갱신하며 앱 소스, 이미지, 네이티브 바이너리, 기기 기록, 인증정보는 공개 패키지에 추가하지 않습니다. `MANIFEST.sha256.json`을 재생성해 정확한 공개 파일과 해시를 검증합니다.

공식 초기화 규칙을 직접 선택한 게임만 원격 출석 알림 대상입니다. `attendanceDeliveryMode=remote`를 사용자가 선택하고 계정·게임·출석 알림·구독을 모두 활성화해야 합니다. 기본값은 local입니다. 동일 서버 출석 기간의 중복 키와 최대 3회 장부를 저장하고 발송 직전 완료 기록을 다시 확인합니다. 게임 실행 기록은 운영체제의 관측 보고이며 실제 게임 서버 로그인이나 보상 수령 증명이 아닙니다. 일일 퀘스트 알림은 생성하지 않습니다.

출석 집계는 `${gameId}_${resetRuleId}_${localDate}`를 공통 문서 ID로 사용합니다. 기존 기록을 다른 서버로 소급 변경하지 않습니다. 개인 raw 실행 및 알림 receipt는 30일, 발송 제한 장부는 35일 보존하며 출석 일별 집계는 사용자가 삭제할 때까지 유지합니다. 기존 관리 작업에서 24시간마다 제한된 20사용자/100문서 범위를 커서로 이어 처리합니다. Firestore TTL/PITR/유료 백업은 사용하지 않습니다.

무료 Spark·결제 미연결 guard, 작업별 읽기/쓰기 제한 및 GitHub 실행 지연 조건은 유지합니다. 원격 발송 성공과 운영체제 표시 요청, 실제 사용자 수신 확인을 서로 다른 상태로 기록합니다. 네이티브 실기기 결과나 새로운 예약 실행은 로컬 모의 테스트로 대신 판정하지 않습니다. 명조의 서버 시간 04:00 안내는 공식 공지에서 확인했지만 Asia 시간대가 공식적으로 검증되기 전에는 해당 출석 규칙을 운영 데이터에 확정하지 않습니다.

이 문서는 **공개 전 검토안**입니다. `outputs/orbit-free-worker-source.zip`만 만들며 GitHub 저장소 공개 전환·push·IAM/키 생성·Auth 설정·FCM 발송은 실행하지 않았습니다. 기존 `xhi719/orbit.` 저장소는 비어 있는 private 상태입니다. 공개 전환은 이번 ZIP의 내용 검토와 사용자의 실행 시점 승인 후에만 진행합니다.

현재 Firebase `orbit-7c729`는 2026-10-11 13:29:48 UTC 읽기 전용 검사에서 billingEnabled=false, 연결 billing account 없음, 기본 Seoul Native Standard 무료 DB를 확인했습니다. worker는 매 실행 BillingInfo를 읽고 이 조건이 불명확하거나 billing이 연결되면 쓰기·발송을 시작하지 않습니다. 계획이 비용 측정이나 무중단 운영을 보장하는 것은 아닙니다.

## 검토 대상 파일

| 파일 | 목적 |
| --- | --- |
| `.github/workflows/firebase-free-collector.yml` | 공개 저장소·활성화 변수 guard, 15분 증분/매일 재검증, 명시 manual backfill |
| `services/firebase/collector.py` | 137 후보/49 검토 출처 seed, 72h 수집, SHA 중복·변경 저장, source health |
| `services/firebase/store.py` | Admin SDK, Spark/Billing guard, atomic lock, quota soft cap |
| `services/firebase/admin.py` | 관리자 요청 재검증·처리, 요청된 계정 삭제 |
| `services/firebase/notifications.py` | 등록/유형/지역/DND 재검사, data-only FCM, recipientUid 격리; TASK_REMINDER 차단 |
| `services/firebase/requirements.txt` | firebase-admin 7.1.0, tzdata 2025.3 |
| `services/firebase/tests/test_firebase.py` | 외부 호출 없는 정책·실패·격리·재검증 복구 회귀 84개 |
| `services/firebase/import_pending_pickups.py` | 사전 조회 원문으로 명시 검토된 older notice/pending outfit만 별도 보완; cron에서 실행하지 않음 |
| `services/collector/engine.py` | HTTPS 승인 출처 fetch/검증 공통 함수 |
| `services/collector/official_adapter.py` | Steam publisher feed 및 공식 YouTube Data API, 게시자·영상·업로드 재생목록·동일 영상 이미지 검증 |
| `services/collector/backfill.py` | 현재 Firebase collector가 import하는 retry_after_seconds 및 transitive helper |
| `services/collector/staging_store.py` | backfill의 transitive import; package workflow는 SQLite staging을 실행하지 않음 |
| `data/game-candidates.json` | 공식 출처 근거가 있는 공개 게임 메타데이터 137개; 이미지 URL은 메타데이터만 |
| `data/official-sources.json` | 전체 후보별 상태 및 검토 Steam source 49개; fixture·개인정보·키 없음 |
| `services/firebase/iam/orbit-free-worker-role.json` | 신설 예정 custom role의 검토용 권한 목록 |
| `services/firebase/iam/orbit-free-fcm-sender-role.json` | 선택 FCM send-only custom role 검토안 |
| `docs/github-free-runner-plan.md` | 이 검토 계획 |
| `README.md`, `.gitignore`, `MANIFEST.sha256.json` | 패키지 전용 실행 안내·개인파일 제외·파일 SHA/bytes 목록 |

이 목록 밖의 앱 UI, 네이티브 바이너리, 게임/브랜드 이미지, 폰트, 개인 문서/토큰/로그, 실제 수집 raw 데이터, SQLite DB, `.env`, `work`, node_modules, ADC, 서비스 계정 키는 제외합니다. Python 코드 안의 alice/mallory 등은 모의 테스트 fixture입니다. source 메타데이터의 공개 기업/게임 명칭은 실제 사용자 계정 데이터가 아닙니다. 공개 가시성 승인이 코드 재사용 라이선스 부여를 뜻하지 않으며 패키지에 임의 MIT 라이선스를 붙이지 않습니다.

## 무료 실행 조건과 범위

GitHub의 **공개 저장소 standard Linux runner**는 무료입니다. private GitHub Free는 2,000분/월이며 15분 주기만으로도 최소 약 2,880분/30일이 필요합니다. 공개 runner 자격을 확인하지 못하면 이 경로를 켜지 않습니다. [공식 과금 정책](https://docs.github.com/en/billing/concepts/product-billing/github-actions).

workflow의 job 조건은 `!github.event.repository.private && vars.ORBIT_FIREBASE_ENABLED == 'true'`입니다. 미설정·private 상태는 skip이고 checkout/worker/Secret 사용을 하지 않습니다. cron은 `7,22,37,52 * * * *`와 매일 `53 4 * * *` UTC입니다. 겹침은 concurrency group으로 직렬화하고 timeout은 15분입니다. scheduled 실행은 default branch의 파일이 필요합니다. cron 지연·누락, 공개 저장소 60일 무활동 자동 비활성화 가능성이 있으며 정확한 15분 전달 SLA를 보장하지 않습니다. [공식 schedule 제약](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

대형/유료 runner, artifact upload, Actions cache, Cloud Run/Functions/Scheduler/App Hosting/Cloud Storage, TTL/PITR/managed backup, billing 연결을 생성하지 않습니다. 이미 있는 Spark Firestore·Auth·FCM만 연결합니다. 원문 49개 Steam feed의 최근 72시간 확인은 전체 137종 모든 공지 채널의 포괄 수집이 아닙니다. 실제 최초 적재 결과는 공지 5개 pending, 확정 행사·예약·픽업 일정 0개입니다.

daily reverify는 source별 최소 15분 간격·백오프를 동일하게 적용합니다. 04:52 증분 수집 직후 04:53 재검증 요청은 `DEFERRED`될 수 있습니다. 이번 패키지는 이를 다음 실행으로 이어갑니다. 원자적 lease 획득에서 먼저 source 간격·백오프·현재 lease를 검사하고, 이미 실행 가능한 증분 출처의 `lastReverifiedAt`이 없거나 24시간 도래했으면 실제 모드를 reverify로 선택합니다. 최근 feed 최대 50건을 확인하되, 72시간 창 밖의 공지는 source별 기존 이벤트 ID query 최대 50건에 포함된 원문만 재검증합니다. 과거 unseen 원문은 신규 삽입하지 않고 skipped 지표에 남깁니다. 읽기 soft cap을 준수하며 query 한도 도달 여부를 기록하므로 전체 과거 이벤트 재검증 완료를 뜻하지 않습니다. 성공할 때만 마지막 재검증 시각을 갱신합니다. 연기 후 다음 due 실행, 24h 경계, 성공 시각, 실패 백오프, 과거 대량 신규삽입 차단 및 기존 모드 범위를 모의 테스트로 검증했습니다. 실제 GitHub daily 실행·정확한 24시간 SLA는 미검증입니다. 최초 적재 건수는 해당 시점의 저장 결과이며, 이전 공지에 진행 중인 일정이 없는지 전체 판정한 결과가 아닙니다.

별도 수동 검토 helper는 실제 publisher response를 다시 파싱하고 기존 최신공지의 source/game 관계를 확인한 뒤 이전 원문 1개와 pending outfit 5개를 create-only로 보완할 수 있습니다. 원문 파일은 이 패키지에 포함하지 않습니다. 출석/재화 임무 알림은 계속 차단하며, 의상은 `outfits[]`에 저장하고 characters/weapons로 바꾸지 않습니다. 시작 시각·지역은 미확인 상태이며 종료 표현에 명시된 연도가 없으면 `endsAt=null`을 유지합니다. 이 helper는 cron에서 자동 실행하지 않습니다.

Firestore 무료 한도는 읽기 50k/쓰기 20k/삭제 20k 일별, 저장 1GiB, 전송 10GiB/월이며 Pacific 자정 무렵 일별 재설정입니다. [공식 한도](https://firebase.google.com/docs/firestore/quotas). 작업별 읽기 2,500·쓰기 500, 관측 worker 누적 읽기 35k·쓰기 14k/day에서 중단합니다. 49 source를 매 tick 성공 처리해도 lock/health/run 기록 쓰기가 발생하므로 변경이 없는 원문이라고 DB 쓰기 0은 아닙니다. 이 보수적 cap에 따라 하루 일부 cron이 중단될 수 있습니다. usageSnapshot의 70/85/95는 worker 추정이며 사용자/콘솔·다른 서비스·전송/저장량 전체를 측정하지 않습니다. 무료 hard quota와 billing 미연결을 유지하고, 한도 부족 시 간격/범위를 줄이지 유료로 전환하지 않습니다.

## 신설 예정 전용 서비스 계정과 최소 권한

소유자의 로컬 `authorized_user` ADC/refresh token을 GitHub에 올리지 않습니다. 공개 승인 뒤 별도로 검토한 전용 계정(예정 이름 `orbit-free-worker`)을 사용합니다. 아래 JSON은 **아직 생성/할당되지 않은** custom role 검토안이며 새로운 자격증명으로 실제 실행해 검증한 상태가 아닙니다. Admin SDK는 Security Rules를 우회하며 project/database의 개인 데이터도 처리할 수 있으므로 신뢰된 default branch 코드에만 접근을 허용해야 합니다.

| 예정 역할 | 정확한 권한 | 필요한 동작 |
| --- | --- | --- |
| `projects/PROJECT_ID/roles/OrbitFreeWorkerDataAuth` | `datastore.databases.get`, `datastore.entities.get/list/create/update/delete` | 문서 query·atomic transaction·CRUD 및 삭제 |
| 같은 역할 | `firebaseauth.users.get/update/delete` | 요청자의 admin claim 재검사, 삭제 계정 disable/revoke/final delete |
| 같은 역할 | `resourcemanager.projects.get` | 읽기 전용 BillingInfo 검사 |
| 선택 `projects/PROJECT_ID/roles/OrbitFreeWorkerSender` | `cloudmessaging.messages.create` | 명시 활성화된 FCM 송신만 |

[Firestore IAM](https://firebase.google.com/docs/firestore/security/iam), [Auth 역할](https://docs.cloud.google.com/iam/docs/roles-permissions/firebaseauth), [BillingInfo 필수 권한](https://docs.cloud.google.com/billing/docs/reference/rest/v1/projects/getBillingInfo), [FCM 권한](https://docs.cloud.google.com/iam/docs/roles-permissions/firebasecloudmessaging). custom permission의 지원·실제 IAM 적용·새 계정 실행은 미래 연결 단계에서 확인합니다.

custom role 대신 predefined 역할을 선택한다면 `roles/datastore.user` + `roles/firebaseauth.admin`, 송신을 켤 때만 `roles/firebasecloudmessaging.admin`이 기능상 대안입니다. 이 predefined Auth/FCM 역할은 필요 이상으로 넓으므로 위 send-only/user-only custom 권한안을 우선합니다. Auth/FCM predefined에는 project.get도 포함되므로 별도의 Project Viewer는 불필요합니다. `roles/owner`, `roles/editor`, `roles/firebase.admin`, service-agent role, Billing Account User/Project Billing Manager, `resourcemanager.projects.createBillingAssignment`, `billing.resourceAssociations.create`, 서비스 생성/활성화/IAM관리 권한은 worker에 부여하지 않습니다. 다른 project나 billing account에 권한을 부여하지 않습니다.

## 미래 연결 값과 실행 순서

| GitHub 설정 | 값/의미 |
| --- | --- |
| Variable `FIREBASE_PROJECT_ID` | 연결 승인된 실제 project ID |
| Variable `ORBIT_FIREBASE_SPARK_CONFIRMED` | 실제 무료/미연결 billing 확인 후 `true` |
| Variable `ORBIT_FIREBASE_ENABLED` | 마지막 검토/수동 시험이 끝날 때까지 미설정 또는 `false` |
| Variable `ORBIT_FIREBASE_NOTIFICATIONS_ENABLED` | 실제 자기 기기의 토큰/수신 검증 전 `false` |
| Secret `FIREBASE_SERVICE_ACCOUNT_JSON` | 새 전용 서비스 계정 JSON; 파일·키 본문을 repo/로그/artifact에 넣지 않음 |

현재 adapter는 전용 JSON 키를 사용합니다. OIDC/Workload Identity는 이번 패키지에 구현·검증하지 않았으므로 keyless 인증이라고 주장하지 않습니다. 새 JSON의 project_id는 런타임 project와 일치해야 합니다. 최소 권한·key 보호/폐기·회전을 검토하고 GitHub Secret 마스킹에 의존해 로그에 출력하는 코드를 만들지 않습니다. 공개 저장소는 일반 fork/PR을 이 workflow로 실행하지 않으며 신뢰된 default branch의 변경을 검토합니다. Settings의 변수 하나만 켜도 실행될 수 있으므로 키 설치 전 코드/SHA와 default branch를 다시 확인합니다.

1. 사용자가 ZIP·공개되는 파일 목록을 검토하고 **실행 시점 공개 전환**을 승인합니다.
2. 승인된 파일만 기존 빈 저장소에 게시하고 공개 standard runner 무료 자격·default branch를 확인합니다. 이 단계는 아직 실행하지 않았습니다.
3. 별도로 전용 서비스 계정·최소 IAM·Secret를 승인·구성합니다. 기존 owner ADC는 복사하지 않습니다.
4. `ORBIT_FIREBASE_ENABLED=false` 상태에서 새 계정으로 BillingInfo/권한/soft cap을 검증합니다. 수동 검증 결과를 확인한 뒤 변수 활성화를 별도로 결정합니다.
5. 최초 seed/backfill은 이미 적재된 값을 중복 생성하지 않으며 새 source seed는 기본 disabled입니다. 새로운 출처는 terms/host/feed/appid 검토 뒤만 명시 활성화합니다.
6. 실제 자기 대상 token이 등록되기 전 송신 변수는 false입니다. provider_accepted와 사람이 실제 수신 확인한 상태를 분리합니다. 현재 실제 FCM 수신은 미검증입니다.

패키지에서 외부 서비스 없이 실행할 로컬 명령:

```sh
python -m unittest discover -s services/firebase/tests -p test_firebase.py -v
python services/firebase/collector.py
python services/firebase/collector.py --seed --report reports/seed-dry-run.json
python services/firebase/admin.py
python services/firebase/notifications.py
```

운영이 별도 승인·구성된 뒤 workflow가 쓰는 정확한 명령은 다음과 같습니다. 이번 검토 작업에서는 실행하지 않습니다.

```sh
python -m pip install -r services/firebase/requirements.txt
python services/firebase/admin.py --apply
python services/firebase/collector.py --mode incremental --hours 72 --live --apply
python services/firebase/collector.py --mode reverify --hours 72 --live --apply
python services/firebase/notifications.py --apply
```

`MANIFEST.sha256.json`은 payload 파일별 SHA256/bytes를 기록하며 자기 파일의 hash는 포함하지 않습니다. 별도 ZIP 검증 보고서는 ZIP 전체 SHA/bytes, archive 허용 목록, secret scan, 추출본 모의 테스트 결과를 기록합니다. 이 검사가 숨은 비밀값이 절대 없다는 수학적 보장이나 실제 GitHub/IAM 실행 검증은 아닙니다.

## 2026-10-14 공식 신규 공지 알림 추가

`announcement:published`는 행사·예약의 날짜 기반 `event:start` 등과 다른 작업입니다. 승인된 공식 출처의 실제 원문 수집 증거와 원문 SHA·제목에 연결된 `main-character` 또는 `main-event` 검증이 있어야 생성합니다. 미확정 날짜를 확정하지 않고 원본 공지 상세로 이동합니다. 원문 변경 시에만 `announcementRevision`을 증가시키고 추출 정책·번역·진행 상태 변경은 같은 공지를 다시 발송하지 않습니다. 명시된 게시·수정 시각이 없으면 관찰 시각을 대신 사용하지 않습니다.

승인된 `calendarRolePolicy.automaticRules`의 `explicit_main_role_extraction_v1`은 게임·수집 어댑터·공식 형식 검토 URL·검토 시각을 묶습니다. 신규 최고 등급 캐릭터는 공식 희귀도 규칙과 같은 문장의 신규·등급·캐릭터·단일 이름 근거 및 픽업 문맥이 있어야 독립 공지로 분류합니다. 복각·무기·혼합 문장은 제외합니다. GF2 메인이벤트는 별도 승인된 `gf2_story_event_v1`의 이름이 일치하는 명시 Game Modes 행사 문장만 지원합니다. 보상·출석·상점 등 일반 제목만으로 메인이벤트를 판단하지 않습니다. 2026-10-14 HAOPLAY 공식 확률 공시의 두 인형 범주에 근거해 `Elite`/`정예` 희귀도 규칙을 별도 검토했습니다. 최고 희귀도 해석과 영어 별칭 대응에는 추론이 포함되며 SSR·5성 별칭을 추가하지 않습니다. 현재 HAOPLAY 공지에 신규 사실을 추정해서 붙이지 않습니다. 다른 출처도 개별 형식 승인이 필요합니다.

사용자의 등록 게임, 전체·게임 알림, `event` 유형, `eventEnabled`, `announcementEnabled`, 카테고리, 관심 지역, 방해 금지 시간을 확인합니다. 지역 `UNSPECIFIED`를 한국이나 전 세계 서버로 변경하지 않으며 `globalAnnouncementsEnabled=true`로 명시 동의한 경우에만 별도 지역 미확정 소식 알림을 보냅니다. `announcementSubscribedAt`은 서버 시각으로 저장합니다. 서버 전용 `users/{uid}/announcementNotificationState/{gameId}`의 최초 실행·재활성화 기준 시각 이전 공지를 제외해 최근 72시간 최초 적재의 알림 폭탄을 막습니다. 첫 작업 실행 이전의 공지를 소급 발송하지 않습니다. 계정 삭제 작업은 이 상태 컬렉션도 지웁니다.

작업 고유 키는 사용자·이벤트·공지 내용 revision·작업 유형입니다. 발송 직전 최신 출처 승인, 원문 증거, 역할, 등록 게임, 구독·알림 설정을 다시 확인하며 등록 해제 또는 변경된 공지의 기존 작업은 취소합니다. 각 기기 제공자 요청 전에도 재검사합니다. FCM data에는 `scope`와 `announcementRole`을 포함합니다. 공급자 수락, 앱 내 알림함, 실패 격리, 사람이 누른 수신 확인을 구분하며 이 변경을 실기기 수신 성공으로 판정하지 않습니다.

무료 예약 작업의 한 번 실행은 최근 게시·수정 72시간의 최신 공지 최대 200건, 사용자 최대 50명, 사용자 등록 게임·기존 작업 각 최대 100건, 작업의 기기 구독 최대 20개 범위입니다. 공지 조회가 200건에 도달하면 `announcementScanLimitReached`를 기록합니다. 한도 도달·예약 지연·할당량 중단으로 모든 알림 전달을 보장하지 않습니다. 기존 Spark/Billing guard·읽기 2,500·쓰기 500의 실행 제한과 무료 사용량 제한을 유지하며 유료 API·리소스를 추가하지 않습니다.

추가된 `newCharacterFormat=gf2_new_doll_block_v1`은 `- New Doll -` 또는 `- New Dolls -` 제목 바로 다음의 정확한 `Elite Doll [이름]` 항목과, 같은 기사에서 그 이름의 지정 발주 확률 상승 문장을 모두 요구합니다. 별도 출처 형식 승인이 없으면 실행하지 않습니다. 복각·무기·표준 등급·여러 역할이 섞인 문장·MOD로만 얻는 인형은 새 픽업으로 추정하지 않습니다. 이 근거는 신규 픽업 사실이며 모집 기간을 증명하지 않으므로 부모 기사의 행사 날짜를 캐릭터 이벤트나 pickupSchedules에 복사하지 않습니다. 같은 줄의 신규 캐릭터도 독립 모집 제목·섹션의 기간이 확인되지 않으면 부모 이야기 행사 날짜를 물려받지 않습니다. 취소 상태는 유지합니다. 현재 로컬에 검토한 Sunborn Steam 3347400 기사에는 New Doll 형식이 있지만 운영 등록·승인된 출처는 아니므로 활성화하지 않았습니다. 운영 HAOPLAY Steam 3308670 현재 기사의 이름 나열만으로 신규를 판단하지 않습니다.

## 2026-10-14 10월 공식 YouTube 소식 수집 정책

기존 공개 payload 19개와 manifest 범위를 유지하면서 공식 YouTube Data API 어댑터를 추가합니다. 블루 아카이브·원신·소녀전선2·니케·명조의 한국어 공식 채널은 배포사 링크와 실제 채널 ID를 대조했고, 업로드 재생목록은 실제 `channels.list` 응답으로 확인했습니다. 검토 source seed는 계속 disabled이며 원격 활성화·적재·예약 실행은 별도 실행 기록으로 검증합니다. 다섯 채널의 지원을 전체 등록 게임·전 세계 전체 소식 수집 완료로 표현하지 않습니다. Atom 실패·빈 WebSub topic을 정상 공지 0건으로 오인하지 않습니다.

`contentPublishedNotBefore=2026-10-01T00:00:00+09:00`을 원문 게시 시각에만 적용합니다. YouTube는 `contentDetails.videoPublishedAt`이며 재생목록 추가·수정·행사 시각으로 대체하지 않습니다. 10월 최초 적재는 별도 `content-backfill`이고 이후 증분은 최근 72시간과 고정 하한을 함께 적용합니다. 페이지 제한·실패·게시 시각 불명은 각각 기록합니다. 10월 이전 자료 정리는 공용 이벤트·정확히 연결된 픽업·원문 문서만 SHA 검토 계획과 변경 전 fingerprint 재검사로 처리하며, 사용자 계정·게임 등록·실행 기록·출석 집계·기기·설정은 포함하지 않습니다. 컬렉션당 검사 500·실행당 삭제 400 제한 뒤 재조회가 필요합니다.

서버는 GitHub Secret `YOUTUBE_API_KEY`를 승인된 `www.googleapis.com/youtube/v3/playlistItems`에만 헤더로 전송합니다. 키는 YouTube API로 제한하며 공개 파일·클라이언트·URL query·로그·보고서에 저장하지 않습니다. 결제 미연결 guard와 기존 무료 Firestore 한도를 유지합니다. [공식 playlistItems 문서](https://developers.google.com/youtube/v3/docs/playlistItems/list)의 요청당 1 quota 단위·최대 50개와 [현재 quota 문서](https://developers.google.com/youtube/v3/guides/quota_and_compliance_audits)의 일반 endpoint 기본 10,000단위/일을 기준으로 합니다. 출처당 10페이지·수집 전체 480초·job 15분 제한에서 다섯 채널의 증분 최댓값 계산은 `5 × 10 × 96 = 4,800단위/일`이며 실측이나 강제 전체 일별 cap은 아닙니다. 수동·재검증·실패 호출 및 다른 사용량은 추가로 대조합니다. 한도 초과는 보류·백오프이며 유료 연결로 전환하지 않습니다.

같은 영상 ID의 공식 썸네일 URL과 출처 증거만 Firestore에 저장합니다. 이미지 blob·base64·Cloud Storage는 사용하지 않습니다. 썸네일은 영상 소식 이미지이며 픽업 배너나 행사 포스터의 근거로 재사용하지 않습니다. 서버 지역·확정 기간·캐릭터 신규/복각은 별도 검증이 필요합니다. `metadataCheckedAt`/`metadataExpiresAt`을 기록하고 30일 안에 재조회하거나 만료 메타데이터를 제한된 일별 작업으로 삭제합니다. 한도·참조 때문에 남는 문서는 PARTIAL과 관리 조치 필요 상태를 기록하며 전체 정리 완료로 표시하지 않습니다. [공식 메타데이터 보관·표시 정책](https://developers.google.com/youtube/terms/developer-policies), [API 이용약관](https://developers.google.com/youtube/terms/api-services-terms-of-service).

[현재 X API 가격](https://docs.x.com/x-api/getting-started/pricing)의 게시물 읽기는 유료이므로 0원 정책에서 자동 탐색은 비활성 상태입니다. 알려진 공식 URL의 oEmbed 읽기만으로 새 소식이나 사진을 자동 수집했다고 판정하지 않습니다. 시험 응답에는 사진 URL이 없었습니다. 비공식 proxy·로그인 제한 우회·유료 API를 사용하지 않습니다. 이번 정책의 실제 운영 적재·삭제 건수·예약 수집 로그는 각 실행 보고서로 따로 제출합니다.

로컬 검증: `python -m unittest discover -s services/firebase/tests -p test_firebase.py -q` **348/348 통과**(기존 283 + 신규 공식 공지 65). 별도 검토 콘텐츠 시험 4개·표시 시험 11개도 통과했습니다. 모든 신규 발송 검증은 MemoryStore와 모의 제공자이며 실제 FCM·운영 사용자 데이터 접근 0입니다. 인자 없이 `notifications.py`는 계속 `DISABLED`, 발송 0, 원격 쓰기 0입니다. 배포·정책 반영·실제 예약 실행·실기기 수신은 운영 실행 자료로 별도 확인해야 합니다.

## 2026-10-14 이후 게시물 보존·공식 영상 이미지 정책

현재 수집·표시 기준은 `contentPublishedNotBefore=2026-10-01T00:00:00+09:00`(UTC 2026-09-30 15:00)입니다. **원 게시일**이 기준이며, 9월 자료가 10월에 수정되거나 10월 행사 기간을 포함해도 다시 저장하지 않습니다. 게시일·시간대가 확인되지 않거나 미래인 원문은 신규 공개 저장 대상에서 제외하고 수집 지표/검증 대기에 남깁니다. 기존 날짜 불명 자료는 임의로 9월로 판단해서 삭제하지 않습니다. 날짜 기반 알림과 신규 공식 공지 알림도 이전 게시일·날짜 미확인 자료를 새 소식으로 발송하지 않습니다.

초기 정리 범위는 공개 `gameEvents`, 연결된 `pickupSchedules`, 원문 증거 `collectionDocuments`입니다. 실제 스키마에 없는 컬렉션을 추정해 삭제하지 않습니다. `games`, `gameSources`, 인증 계정, 등록 게임, 실행·출석 기록, 사용자 설정·알림함·발송 이력은 보존합니다. 삭제된 이벤트의 발송 대기 작업은 기존 발송 직전 자격 검사에서 취소하며 이 정리 작업 자체는 FCM을 호출하지 않습니다.

정리 dry run은 컬렉션별 최대 500문서를 읽고 삭제 후보 최대 400개를 문서 지문과 함께 제시합니다. 실제 적용에는 그 JSON의 정확한 검토 지문이 필요합니다. 픽업은 연결 공지의 날짜와 관계를 확인하며, 픽업 삭제가 성공하기 전에는 부모 공지를 삭제하지 않습니다. 부모가 남아 있으면 연결 원문 증거도 남깁니다. 적용 순간 transaction에서 문서·관계가 바뀌었으면 삭제를 건너뜁니다. 한도·날짜 불명·충돌·미처리 연결 자료를 기록하고 적용 후 재조회 없이 전체 삭제 완료라고 표시하지 않습니다.

```sh
# 승인된 Spark 연결 환경에서 실제 공개 데이터만 읽는 검토 목록. 쓰기/발송 0.
python services/firebase/collector.py --content-cleanup --live --report reports/content-cutoff-cleanup-dryrun.json
# 검토한 정확한 목록을 별도로 적용. 기존 무료/Billing guard를 계속 검사.
python services/firebase/collector.py --content-cleanup --live --apply --cleanup-review reports/content-cutoff-cleanup-dryrun.json --report reports/content-cutoff-cleanup-applied.json
# 10월 1일부터의 최초 보완 적재. seed/출처 활성화/알림 발송을 포함하지 않음.
python services/firebase/collector.py --mode content-backfill --live --apply --report reports/youtube-october-backfill.json
```

`content-backfill`은 고정 10월 기준부터 실행 시각까지(최대 31일)의 별도 모드이며 정기 daily reverify로 바꾸지 않습니다. 일반 모드의 72시간 제한은 유지합니다. 운영 `run`은 관리자 승인·활성화된 DB 출처를 사용하므로 registry 파일을 지정했다고 운영 수집 범위가 그 파일로 제한되는 것은 아닙니다. 게임별 한정 수동 적재는 동일 승인·lease·quota를 검사하는 `collect_one`을 정확한 출처 ID에 대해 호출해야 합니다. 출처 한 번 실행은 10페이지·최대 480초, worker 최대 50개 실수집·200개 출처 조회, 기존 읽기 2,500/쓰기 500 및 일별 soft cap을 유지합니다.

공식 YouTube 채널의 공개 ID·publisher 관계와 실제 `channels.list`의 `relatedPlaylists.uploads`를 별도로 확인한 뒤 `youtube_data_api` 출처를 등록합니다. 채널 ID의 UC를 UU로 문자열 치환해 uploads ID를 만들어 쓰지 않습니다. `youtubeUploadsPlaylistVerifiedAt`과 별도 API 이용 조건 검토가 있어야 합니다. 출처 신규 등록은 계속 disabled이며 승인된 공식 출처만 활성화합니다. Atom `/feeds/videos.xml` 404나 `/xml/feeds/videos.xml`의 채널 정보 없는 정적 템플릿을 빈 정상 피드로 보고하지 않습니다.

무료 YouTube Data API의 `playlistItems.list`로 업로드를 조회하고, 분류된 공식 방송 예고에 한해 `videos.list`의 `snippet,liveStreamingDetails`를 추가 조회합니다. 영상 ID는 최대 50개의 고유한 확인된 ID이며 검색·소유자 전용 필드·개인 계정 API는 호출하지 않습니다. `YOUTUBE_API_KEY`는 서버 환경/GitHub Secret에만 보관하고 승인된 `www.googleapis.com/youtube/v3/playlistItems` 또는 `/youtube/v3/videos` 요청 헤더로 전달합니다. 키·인증 헤더는 공개 source URL, 원문 데이터, 저장소, 결과 로그에 넣지 않습니다. 인증 요청 redirect와 중복 query는 차단합니다. 키가 없으면 해당 출처만 보류하며 다른 게임의 수집을 계속합니다. 게시일은 `contentDetails.videoPublishedAt`을 쓰고 재생목록에 넣은 시각 `snippet.publishedAt`은 별도 증거로 저장합니다. 다른 채널·영상·재생목록 응답은 차단합니다.

영상 썸네일은 그 영상 ID의 공식 응답 URL만 저장하며 이미지 바이트를 파일이나 Firestore에 넣지 않습니다. `noticeImage`는 `NOTICE_ART`/`source_post`와 독립 루트 채널·영상 ID, 원문 SHA·제목·정식 영상 URL을 연결한 **공식 영상 소식 이미지**입니다. 저작권 사용 허가를 추정하지 않고 `UNCONFIRMED` 원본 참조로 표시합니다. 요청한 유형의 소식과, 같은 게시물의 일정이 공식적으로 확인된 달력 상세에는 공식 YouTube 캡션·영상 링크와 함께 표시할 수 있습니다. 픽업 배너·별도 캐릭터 게시글의 증거로 승격하거나 다른 게시물에 복제하지 않습니다. 원문 썸네일 변경 시 실제 응답에 맞춰 갱신합니다.

실제 검사에서 일부 `fhddefault.jpg` URL은 HTTP 200이지만 API의 1920×1080 설명과 다른 120×90 회색 대체 이미지를 반환했습니다. `youtubeNoticeImagePolicyVersion=2`는 API가 이미 제공한 `maxres`, `standard`, `high`, `medium`, `default`를 우선하며 FHD/QHD/UHD는 뒤에 검토합니다. 제목이 같은 기존 원문도 이 출처 정책 지문 변경으로 다시 검사하므로 잘못 저장한 FHD 링크가 unchanged로 남지 않습니다. Steam 등 다른 출처의 추출 버전은 이 영상 이미지 선택 변경만으로 바꾸지 않습니다. 현재 정책 버전은 3이며, 공식 API가 실제 제공한 같은 영상의 `maxresdefault_live.jpg` 등 `_live` 변형도 허용합니다. URL을 생성하지 않고 원 응답 목록·실제 이미지 크기 검증을 똑같이 적용하며, 버전 2에서 미지원이던 방송용 링크도 같은 원문 SHA에서 다시 검사합니다.

신규·변경 원문 및 실제 재조회 24시간 경과 때만 공식 응답의 이미지 후보 최대 3개를 메모리에서 확인합니다. 후보당 최대 2MiB, HTTPS의 해당 영상 CDN URL, HTTP 200 JPEG/PNG, 실제 헤더 크기가 API 제공 width/height와 일치하고 최소 320×180이어야 합니다. 공지 전체 480초 deadline 안에서 검사하며 인증 키를 이미지 CDN에 보내지 않습니다. bytes를 보관하지 않고 SHA256·실제/예상 크기·확인 시각의 `imageVerification` 증거만 저장합니다. Firestore transaction 밖에서 한 번 검사해 transaction 재시도로 HTTP가 반복되지 않게 합니다. 모든 후보가 실패·미확인·시간 초과면 이전 `noticeImage`/공지 사진을 지우고 오류·보류 사유를 기록합니다. 게임 로고·다른 게시물 사진으로 대신하지 않습니다. 동일 검증 원문을 24시간 안에 반복 조회할 때는 추가 이미지 요청을 하지 않습니다.

YouTube의 비인증 메타데이터는 실제 재조회 시각 `metadataCheckedAt`과 30일 후 `metadataExpiresAt`을 저장합니다. 같은 원문을 재조회하더라도 24시간이 지난 경우에만 보존 기한 갱신 쓰기를 실행하며 수집하지 않은 영상의 기한은 늘리지 않습니다. worker `run`은 하루 한 번 공개 영상 소식 50건·원문 증거 50건의 만료 자료만 정리합니다. 정확한 공식 영상 ID/URL·30일 시각·현재 지문을 재검사하고, 미검증 관계가 있으면 보류합니다. 이 자동 만료 정리는 최초 10월 이전 자료 삭제와 별개이며 개인 데이터 읽기/발송 0입니다. UI는 만료된 영상 소식을 제외합니다. [YouTube API 정책](https://developers.google.com/youtube/terms/developer-policies).

`playlistItems.list`는 페이지당 1 quota unit입니다. 정상 응답 10페이지 한도 도달은 `SUCCESS_BOUNDED`/`PAGE_LIMIT_REACHED_WITH_MORE_UPLOADS`로 저장한 범위를 보고하며 실패 백오프를 만들지 않습니다. 전체 월 수집 완료로 간주하지 않고 `windowCoverageVerified=false`를 유지합니다. 반복 token·identity 오류·HTTP/할당량 오류는 실패와 백오프로 기록합니다. 재생목록 순서가 실제 게시 순서와 같다고 가정해서 오래된 영상 한 건만으로 페이지 조회를 조기 종료하지 않습니다. 최대 6개 채널×10페이지×96회면 5,760 quota unit/day이며 다른 API 요청 사용량은 별도입니다. 기본 무료 quota 소진 시 제한하고 결제 연결·quota 증액·유료 전환을 실행하지 않습니다. [공식 quota 비용](https://developers.google.com/youtube/v3/determine_quota_cost).

출처가 50개보다 많아도 최근 실행한 출처가 계속 앞을 점유하지 않도록 미실행·오래된 `lastCheckedAt` 출처를 먼저 검토합니다. backoff/미도래 출처는 조회된 `nextRunAt`으로 보류하고 50개 실수집 슬롯을 소모하지 않습니다. 기존 atomic lease가 최종 실행 가능 여부를 결정합니다.

X 게시물 수집은 이번 worker에 구현·활성화하지 않았습니다. 공식 API 접근·비용·publisher/media 증거가 연결되지 않은 상태에서 임의 scraper나 다른 이미지로 대신하지 않습니다. YouTube 역시 검증한 출처와 반환한 범위만 수집하며 모든 게임·모든 공식 SNS의 완전한 자동 수집을 주장하지 않습니다.

최신 로컬 검증은 Firebase worker **447/447**, collector **107/107**입니다. 기존 348개 중 이전 게시물의 최근 수정으로 범위에 포함하던 YouTube 시험 1개는 사용자 요청한 10월 게시일 기준에 맞게 제외 검증으로 변경했고, 99개 검증을 추가했습니다. 여기에는 API adapter/키 전송 차단, 날짜·이미지·원본 증거, 정리 지문/연결 관계, 만료·재검증, 공정 순서, 삭제 공지 발송 중지, 실제 크기 불일치 대체 이미지 차단 및 이미지 요청 한도 등이 포함됩니다. 이미지 시험은 합성 JPEG/PNG/WebP 헤더와 모의 응답만 쓰며 실제 이미지 파일을 테스트에 복사하지 않습니다. 이 결과는 모의 API/MemoryStore이며 실제 API 연결·운영 DB 적재·정리·GitHub 예약 실행·기기 수신은 각 운영 보고서에서 별도로 확인합니다.


## 2026-10-14 PV·예고 분류와 달력 연결

모든 영상을 가져와 표시하지 않습니다. `contentClassificationVersion=1`은 제목과 원문 SHA에 연결된 `contentKind`를 기록합니다. 캐릭터 PV·이벤트 PV는 소식 전용입니다. 특별 방송·오프라인 행사·픽업·이벤트 예고는 소식과 일정 확인 후 달력 대상으로 분리합니다. OST·일반 쇼츠·생일·컷신·공략·지난 방송은 표시 대상에서 제외하고, 제외된 신규 영상은 공개 공지·원문 저장과 이미지 요청을 실행하지 않습니다. 명시적 OST 콘서트/음악회 개최 예고는 오프라인 유형으로 처리합니다.

모든 표시 대상은 같은 게시물의 검증된 대표 사진이 필요합니다. `newsEligible`와 `calendarEligible`는 단순 관리자 플래그가 아니라 원문 분류·기간·이미지 증거를 다시 검사한 결과입니다. PV에 기간이나 잘못된 calendarEligible 값이 있어도 달력에 넣지 않습니다. 기존 공지는 개인 기록을 건드리지 않고 같은 함수로 다시 분류합니다. 새 출처가 등록되면 동일 규칙을 사용하며, 게임 등록 여부와 감지 연결은 별개입니다. 출처가 없는 게임의 소식을 만들어 내거나 전체 게임 자동 수집 성공으로 표시하지 않습니다.

공식 YouTube 방송의 `liveStreamingDetails.scheduledStartTime` 또는 `actualStartTime`은 동일 채널·영상·제목과 대조한 RFC3339 값만 사용합니다. 방송 기간을 추측하지 않으며 저장된 동일 시작/종료 값은 `scheduleKind=point`인 한 시점입니다. 원본 게임 서버 지역은 계속 미확인으로 두고 `calendarAudience=GLOBAL_ANNOUNCEMENT`인 방송만 허용합니다. API 한 번의 추가 quota 1단위를 실행 지표에 포함합니다. 영상 게시일을 행사 시작일로 쓰지 않습니다.

공식 원문에 확인된 메인 픽업·메인 이벤트 기간이 있고 서버만 미확인인 이전 검토 자료는 정확한 원문 역할·시간대·기간 증거가 일치할 때만 `sourceClockOnly`인 참고 막대로 표시합니다. 검증된 서버 일정 알림으로 승격하지 않습니다. 복각·무기와 중복 부모 공지는 달력에서 제외합니다. 달력에는 실제 시작일에서만 이름을 한 번 표시하고 다음 주·다음 달로 이어지는 막대에는 반복하지 않습니다. 통합 이벤트 캘린더는 등록 게임 중 선택된 게임들의 같은 달력 자료를 사용합니다.
# 2026-10-14 October catalogue expansion

This update keeps the approved 19-file public worker scope. New source reviews cover 56 additional exact YouTube channel/upload identities and three publisher website/RSS adapters. Subscription settings never determine which public game data the collector stores. Shared publisher channels require a reviewed literal game-title filter. October publication bounds are 2026-10-01 00:00 KST through 2026-11-01 00:00 KST exclusive; old posts are not imported merely because an event continues into October. YouTube descriptions are discarded and the visible card contains the exact thumbnail, title and watch link.

The Blue Archive October 13 publisher article is enriched by the exact official Ticketlink notice for four membership/ticket opening stages and the festival poster. The September ticket notice is supplementary evidence and is never inserted as October news. The Japanese Girls' Frontline 2 orchestra release is an October announcement of a January 15, 2027 date; it does not establish a Korean concert or an unpublished hour.

The scheduler still wakes at 7, 22, 37 and 52 minutes. With more than 100 sources, checking every source every 15 minutes exceeds the worker's free daily write budget. The worker allocates source attempts from remaining reads/writes, rotates least-recently-checked sources, persists incomplete monthly backfill cursors, and records deferred quota work. Estimated rotation is not a delivery guarantee. Firebase client traffic is not included in the worker accounting; Spark provider limits can suspend the service. Billing remains disconnected, no paid resources are created, and ambiguous billing verification fails closed.



## 2026-10-14 complete catalogue source review

The existing 137-game catalogue is retained. 45 previously unconnected games were reviewed against primary publisher/homepage/store evidence. The additional disabled configuration contains 45 exact YouTube uploads sources and six public publisher RSS sources, covering 44 of those games. Final Gear EN is corroborated by the exact channel link in its operator's declared game script and by YouTube channel/uploads metadata. Bungo's operator RSS is an old archive with no October 2026 entries observed, checked daily with exact game aliases; source connectivity does not imply current game news. Millennium Tour ELF remains unconnected: no confirmed public feed/API/channel was found and encrypted site configuration is not decoded. Source discovery metadata records access/ownership restrictions instead of claiming complete coverage. Source activation is separate and requires the verified public worker bytes. Shared publisher/IP channels use literal game names and side-game exclusions; dedicated scopes are manually corroborated against exact channel/upload API identity. New RSS sources accept only relevant game announcements, not administrative contact notices. All imported publications remain within October 2026; old identity evidence is not republished as October news. Confirmed ended regional archives may use daily checks. No credentials, image bytes or private user data are added to this public package. Spark headroom and source rotation still apply; exact 15-minute all-source execution and complete monthly history are not promised.

Operational review found an HTTP 403 from the Assault Lily RSS collector request. That RSS is disabled in Firestore; its successful official YouTube path remains enabled. A successful browser/feed review does not override a denied collection request. No header disguise or access bypass is used. The RSS parser now excludes a proven out-of-October publication before validating its legacy article URL. An ambiguous date without a time is omitted only when its whole possible day falls outside the month; no posting clock is invented. In-month articles retain HTTPS, reviewed-origin, aware-time and RDF-identity validation. These guards have synthetic regressions in the existing approved public test file; local archived snapshots and private files are excluded from the public worker.

## 2026-10-15 existing registered game schedule correction

October news and October calendar periods are separate. The user approved an older publisher notice only for a reviewed main period intersecting October; it never becomes October news. Date-only periods preserve their literal source dates and never become clock-based push reminders. Period, exact poster, main-role and auxiliary update-day proofs bind the original URL, title and SHA. Invalid, cancelled, rerun, weapon and unrelated periods cannot survive public-news cleanup as main calendar periods.

Reviewed Genshin and Wuthering Waves public CMS adapters use only website-declared public identifiers and endpoints, preserve date-only publication precision, and never infer a game-server offset or an update-completion clock. Candidate configurations remain disabled until separately activated and checked. Official image-only clocks and relative update dates need source-bound review; a successful public endpoint does not prove a schedule exists.

The current Wuthering Waves Korean main pickup was manually reviewed from its publisher-linked official Naver lounge. Naver's service terms require prior permission for automated post collection, so that source remains manual-only and disabled. No Naver crawler or polling task is enabled. Its exact pickup banner is an original notice reference, with reuse rights unconfirmed; generic header/footer or version artwork never substitutes for it. No credentials, image bytes, app binaries or private records are added to this public package.
