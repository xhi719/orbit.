# 공개 무료 worker 저장소 검토 계획

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
| `services/collector/official_adapter.py` | Steam publisher feed, appid/host/region 원문 검증 |
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
