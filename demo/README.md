# 가상 에피소드로 CHAIN 실행하기

이 데모는 가상 병원의 기록·검사 Event를 CHAIN에 보내고, 콘솔에서 의료진 결정을 입력해
전체 Workflow를 확인한다. Temporal Service·Worker·Signal·Activity·Timer는 실제로 실행한다.
병원 자료와 Agent 출력은 합성이며, 알림과 EHR 표시·취소는 외부 전달 없이 `MOCK_RECORDED`로 기록한다.

모든 명령은 저장소 루트에서 실행한다. Python 3.12를 권장한다.
이 디렉터리와 해당 합성 Episode는 전달본에 포함된다.
State별 처리·선택·전이는 [루트 README의 단계별 안내](../README.md#43-화면을-따라-진행하기),
Agent/API/Notifier 입출력은 [통합 계약 안내](../README.md#6-agent-구현-연결)를 함께 확인한다.

## 1. 설치하고 실행하기

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

아래 명령 하나로 임시 Temporal Service, Worker, 가상 병원과 콘솔을 함께 시작한다.
별도 Worker를 실행할 필요는 없다. 첫 실행에는 SDK가 Temporal 실행 파일을 내려받을 수 있다.

```bash
.venv/bin/python -m demo \
  --backend temporal --start-local \
  --test-mode --test-timer-scale 0.01 \
  --run-timeout 1800 \
  --output-dir "output/demo-$(date +%Y%m%d-%H%M%S)"
```

- `--test-mode`: 합성 시나리오의 가상 시계를 사용한다. 병원 Event는 실제 선행 사건을 기다린다.
- `--test-timer-scale 0.01`: Workflow의 실제 타이머를 데모에서만 100배 줄인다. 재확인 10분은 실제 6초다.
- `--run-timeout 1800`: 데모를 최대 30분 실행한다. 시간 제한이 지나면 부분 결과를 보존한다.
- 출력 디렉터리는 실행마다 새로 지정한다. 이미 있는 디렉터리는 덮어쓰지 않는다.

이미 실행 중인 Service를 사용할 때는 `--start-local`을 다음 옵션으로 바꾼다.

```text
--address 127.0.0.1:7233
```

이 경우에도 데모가 자신의 Worker를 생성한다. 개발 Service를 별도로 준비하는 방법은
[통합 실행 안내](../README.md)에 있다.

## 2. 콘솔에서 확인할 내용

화면에는 현재 State, 자료·Agent 처리 기록, 고정된 HITL 근거, 결정 선택지가 표시된다.
각 자료는 값·상태·출처 버전·기록 시각·CHAIN이 알게 된 시각을 구분한다.
미확보·철회·오류·충돌 자료는 해당 상태로 표시한다.
긴 문서는 기본 화면에서 미리보기로 보여주며, 임상 요약을 새로 추론하지 않는다.

현재 Mock은 특정 합성 입력과 평가시각에 대응한다. 기본 시나리오에서는 다음 표시를 기다린 뒤
결정을 입력한다. 이는 데모의 입력 안내이며 엔진의 임상 전이조건이 아니다.

| 콘솔 표시 | 할 일 |
|---|---|
| `DEMO_DECISION_READY \| HITL_1_PROCEED_TO_CT` | HITL #1 결정 입력. 이 시점까지 독립 CBC/COAG 결과가 유입된다 |
| `DEMO_DECISION_READY \| HITL_2_THROMBOLYSIS` | HITL #2 결정 입력 |

선택 번호 또는 화면의 코드를 입력하고, 모의 역할·ID·사유를 입력한다.
HITL #2의 YES 선택에는 화면에 표시된 네 확인사항을 각각 확인해야 한다.
다른 시각이나 지원되지 않는 입력에 대응하는 fixture가 없으면 `FIXTURE_NOT_FOUND`로 종료한다.

| 입력 명령 | 화면 동작 |
|---|---|
| `docs` | 지금 열린 요청에 고정된 문서 전문 보기 |
| `json` | 지금 열린 요청의 전체 원문 JSON 보기 |
| `q` | 결정을 전송하지 않고 데모 입력 종료 |

열린 HITL의 근거는 이후 정보가 추가되어도 그대로 유지된다. 실시간 Event·Agent 결과는 별도로 표시한다.
입력 중에도 자료와 Agent 응답을 계속 처리한다.

S1 진입에서는 EHR 플래그와 대시보드 표시 활성화, 세 준비 알림이 Mock으로 기록된다.
HITL #1에서 NOT_STROKE_PATHWAY를 선택하면 S1X로 가서 같은 표시를 해제하고,
CT실·간호팀·당직 신경과에 대한 준비 취소 알림을 별도로 기록한다.
SQLite의 최종 Mock 표시 상태는 늦게 완료된 이전 활성화 요청이 취소를 되돌리지 않도록 버전으로 관리한다.
이 과정은 실제 EHR 화면이나 SMS를 변경하지 않으며 이미 보낸 메시지를 회수하는 기능도 아니다.
표시 Adapter의 요청·응답과 실제 연결 위치는 [루트 README 8.3절](../README.md#83-ehr-표시대시보드-활성화와-취소)에 있다.

설정의 State 이름과 설명은 의료진 뷰어의 13개 State 명세를 따른다.
각 명세의 자료요건·유효시간은 현재 확보한 Context의 상태를 보여주는 항목이며,
추가 임상 전이조건을 자동으로 만들지 않는다. 수정 방법은 [YAML 작성 안내](../README.md#92-state-명세와-yaml)에 있다.
Summary 준비가 실패하면 HITL이 열리지 않고, 종료 State에서 완료된 Workflow는 후속 Event를 계속 받지 않는다.

## 3. HOLD/DEFER와 타이머 읽기

HOLD/DEFER 응답이 수락되면 현재 State를 유지하고 재요청 타이머를 기다린다.
승인이나 경로 종료를 자동으로 선택하지 않는다.

타이머에는 진행 막대, 남은 시간, 실행 시계와 원래 설정 시간이 표시된다.
가상 병원의 시계와 Temporal 타이머의 실제 경과시간은 별개다.
화면의 짧은 타이머는 약 1초, 긴 타이머는 약 5초 변화 간격으로 갱신한다.

```text
TIMER | … | ⏱ Temporal Workflow 타이머 · 실제 Temporal 타이머
    [■■■■□□□□□□□□] 남은 00:04 / 전체 00:06 · 대기 중 · 원래 설정 10:00
```

남은 시간이 0이면 실제 발생을 확인하기 전까지 `만료 처리 대기`로 표시한다.
조회가 지연되면 남은 시간을 만들어 채우지 않고 `현재 상태 확인 지연`으로 표시한다.
타이머가 끝나고 새 요청이 열리면 새 Request ID의 근거를 확인해 결정한다.

## 4. 자동으로 전체 경로 확인하기

사람의 입력 없이 정해진 합성 응답으로 연결 상태를 확인하려면 명시적으로 recorded 모드를 사용한다.
이 응답 파일과 기대 경로는 데모만 읽으며 공통 엔진과 Workflow에는 전달하지 않는다.

```bash
.venv/bin/python -m demo \
  --backend temporal --start-local --test-mode \
  --hitl recorded \
  --recorded demo/scenarios/recorded_hitl.json \
  --expected demo/scenarios/expected.json \
  --run-timeout 180 \
  --output-dir "output/demo-check-$(date +%Y%m%d-%H%M%S)"
```

기본 기대 경로는 `S0 → S1 → S2 → S2_1 → S3`다.
성공하면 종료코드 0과 `RUN_OUTCOME | COMPLETED`가 출력된다.
임상 Agent의 실제 성능·치료 적합성을 검증하는 시험은 아니다.

Service 없이 같은 흐름을 확인하려면 `--backend local`을 사용하고 `--start-local`을 뺀다.
이 실행은 Local Engine 확인이며 실제 Temporal 통합시험으로 세지 않는다.

## 5. 결과와 Replay 확인하기

| 출력 파일 | 내용 |
|---|---|
| `outcome.json` | 실행 성공·실패·중단과 최종 State |
| `snapshot.json` | 전체 Context, 현재 State 명세·자료요건 표시, State 경로, Agent 결과, HITL 이력, 표시·취소 receipt와 감사 기록 |
| `manifest.json` | 실행에 고정한 Workflow·Policy·Registry·구현 버전과 데모 설정 |
| `commands.jsonl` | 실제 요청·응답과 Temporal Activity의 실행 ID·재시도 횟수 |
| `publications.json`, `simulation.json` | 자료 공개·Event와 실제 관찰한 가상 병원 기준 사건 |
| `comparison.json` | `--expected`를 지정한 실행의 사후 비교 |
| `temporal_history.json`, `temporal_execution.json`, `replay.json` | Temporal 실행 History와 Replay 결과 |
| `notices.sqlite` | 요청 ID별 Mock 알림·표시·취소 receipt와 최종 Mock 표시 상태 |

Temporal 실행은 종료 시 History를 저장하고 Replay한다. 저장한 History를 별도로 확인하려면:

```bash
.venv/bin/python -m demo.support.replay_history output/확인할실행폴더 \
  --report output/확인할실행폴더/replay-again.json
```

새 보고서 경로를 지정하며 기존 파일을 덮어쓰지 않는다.
콘솔 종료나 실행 시간 제한은 자동 의료진 응답을 만들지 않는다.
임시 Service는 데모 종료 시 함께 내려가고, 기존 Service를 사용한 경우 그 Service는 계속 실행된다.

## 6. 코드와 실제 통합의 경계

| 위치 | 역할 |
|---|---|
| [run.py](run.py) | 데모 구성·수명·실행 결과 저장 |
| [console.py](console.py) | 고정된 HITL 근거 확인과 데모 입력 |
| [presentation.py](presentation.py) | 읽기 쉬운 표시와 진행 로그 |
| [local_runtime.py](local_runtime.py), [temporal_runtime.py](temporal_runtime.py), [timers.py](timers.py) | Local/Temporal 연결과 타이머 관측 |
| [simulator/](simulator/) | 선행 사건과 상대시간을 따르는 가상 병원 |
| [support/](support/) | 가상 시계 Driver, 기록 응답, 사후 비교, Replay |
| [scenarios/](scenarios/) | 데모 시계·기록 응답·기대 결과 |
| [합성 Episode](../episodes/stroke_reference_001_v03/) | 초기 입력, 원천자료·시뮬레이션 일정과 Agent fixture |
| [chain_demo/](../chain_demo/) | 통합 팀이 사용하는 공통 엔진·Worker·Workflow·Activity·계약 |

실제 통합은 `chain_demo.worker`와 백엔드의 Signal/Query 계약을 사용한다.
프론트·백엔드에서 이 데모 콘솔을 호출할 필요는 없다.
구현할 Agent/API와 연결 위치는 [루트 README](../README.md)에 있다.
