
# SCM - 통합 물리 센서 실시간 모니터링 시스템 (Optimized)

## 개요

이 애플리케이션은 다양한 물리 센서(온도, 초음파 거리, 라돈, 자기장 등) 데이터를 실시간으로 수집, 처리, 시각화 및 저장하는 고성능 통합 모니터링 솔루션입니다.

최근 대규모 아키텍처 최적화를 통해, 시스템의 안정성과 성능이 극대화되었습니다. 모든 하드웨어 제어와 데이터베이스 작업은 GUI와 완전히 독립된 백그라운드 스레드에서 비동기적으로 처리되어, 장기간 운영 시에도 최고의 응답성과 안정성을 보장합니다.

## 주요 개선 사항 및 특징

- **고성능 아키텍처 (Optimized Architecture)**:
  - **생산자-소비자 패턴**: 센서 워커(생산자)와 데이터베이스 워커(소비자)를 분리하고 중앙 큐(`Queue`)로 연결하여, 데이터 수집과 DB 저장 부하를 완전히 분산시켰습니다.
  - **GUI 응답성 보장**: 모든 I/O 작업(네트워크, 파일, DB)이 백그라운드에서 처리되어 GUI 멈춤 현상이 발생하지 않습니다.
- **다중 센서 통합**: NI-DAQ (온도, 거리), 시리얼 (라돈), VISA (자기장) 등 다양한 인터페이스의 센서를 동시에 지원합니다.
- **실시간 시각화**: `pyqtgraph`를 활용한 고속 그래프 렌더링.
  - **효율적인 데이터 처리**: 시계열 데이터 버퍼에 `collections.deque`를 사용하여 데이터 추가/삭제 성능을 O(1)로 최적화했습니다.
- **안정성 및 응답성 강화**:
  - **비동기 I/O**: 블로킹 함수(`time.sleep`)를 제거하고 `QTimer` 기반의 이벤트 처리 방식을 도입하여 스레드 응답성을 향상시켰습니다.
  - **오류 복구 로직**: NI-DAQ 수집 중 일시적인 오류 발생 시 자동으로 연결을 재시도합니다.
- **최적화된 데이터베이스 연동**:
  - 전용 `DatabaseWorker` 스레드에서 **배치 처리(Batch Processing)**를 수행하여 DB 삽입 효율을 극대화합니다.
- **유지보수성 향상**:
  - **설정 파일 분리**: 모든 환경 설정을 `config_scm.json`으로 관리합니다.
  - **구조화된 로깅**: 표준 `logging` 모듈을 도입하여 시스템 운영 기록 및 디버깅을 체계화했습니다.

## 시스템 아키텍처 (Optimized)

시스템은 크게 세 그룹의 스레드로 구성되며, 데이터는 시그널(Signal)과 큐(Queue)를 통해 안전하게 전달됩니다.

1.  **메인 스레드 (GUI)**: 사용자 인터페이스 및 그래프 렌더링 담당.
2.  **워커 스레드 (Sensors - 생산자)**: 각 센서 데이터 수집 및 처리. 처리된 데이터는 GUI(시그널)와 DB 큐로 전송.
3.  **데이터베이스 스레드 (DB Worker - 소비자)**: DB 큐에서 데이터를 가져와 배치로 처리 및 저장.

```svg
<svg width="800" height="500" viewBox="0 0 800 500" xmlns="http://www.w3.org/2000/svg">
  <style>
    .boundary { fill: none; stroke: #999; stroke-width: 1; stroke-dasharray: 5,5; }
    .main-thread { fill: #E3F2FD; stroke: #2196F3; stroke-width: 2; }
    .worker-thread { fill: #FFF3E0; stroke: #FF9800; stroke-width: 2; }
    .db-thread { fill: #E8F5E9; stroke: #4CAF50; stroke-width: 2; }
    .queue { fill: #F3E5F5; stroke: #9C27B0; stroke-width: 2; }
    .text { font-family: Arial, sans-serif; font-size: 12px; }
    .title { font-size: 14px; font-weight: bold; }
    .arrow { stroke: #333; stroke-width: 1.5; marker-end: url(#arrowhead); }
    .signal-arrow { stroke: #2196F3; stroke-width: 1.5; stroke-dasharray: 3,3; marker-end: url(#arrowhead-signal); }
  </style>

  <defs>
    <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="10" refY="3.5" orient="auto">
      <polygon points="0 0, 10 3.5, 0 7" fill="#333" />
    </marker>
    <marker id="arrowhead-signal" markerWidth="10" markerHeight="7" refX="10" refY="3.5" orient="auto">
      <polygon points="0 0, 10 3.5, 0 7" fill="#2196F3" />
    </marker>
  </defs>

  <!-- Threads Boundaries -->
  <rect x="10" y="10" width="250" height="480" class="boundary" />
  <text x="20" y="30" class="title">Main Thread (GUI)</text>

  <rect x="270" y="10" width="250" height="480" class="boundary" />
  <text x="280" y="30" class="title">Worker Threads (Sensors - Producers)</text>

  <rect x="530" y="10" width="260" height="480" class="boundary" />
  <text x="540" y="30" class="title">Database Thread (Consumer) &amp; External</text>

  <!-- Main Thread Components -->
  <rect x="30" y="50" width="210" height="50" class="main-thread" />
  <text x="135" y="80" class="text" text-anchor="middle">MainWindow (UI)</text>

  <rect x="30" y="220" width="210" height="50" class="main-thread" />
  <text x="135" y="245" class="text" text-anchor="middle">Update UI Slots</text>
  <text x="135" y="260" class="text" text-anchor="middle">(Using Deque)</text>

  <!-- Worker Threads Components -->
  <rect x="290" y="50" width="210" height="50" class="worker-thread" />
  <text x="395" y="75" class="text" text-anchor="middle">DaqWorker</text>
  <text x="395" y="90" class="text" text-anchor="middle">(Loop + Retry Logic)</text>

  <rect x="290" y="120" width="210" height="50" class="worker-thread" />
  <text x="395" y="145" class="text" text-anchor="middle">RadonWorker</text>
  <text x="395" y="160" class="text" text-anchor="middle">(QTimer - Non-blocking)</text>

  <rect x="290" y="190" width="210" height="50" class="worker-thread" />
  <text x="395" y="215" class="text" text-anchor="middle">MagnetometerWorker</text>
  <text x="395" y="230" class="text" text-anchor="middle">(QTimer - Non-blocking)</text>

  <!-- Database Thread Components -->
  <rect x="550" y="220" width="210" height="50" class="db-thread" />
  <text x="655" y="245" class="text" text-anchor="middle">DatabaseWorker</text>
   <text x="655" y="260" class="text" text-anchor="middle">(Batch Processing)</text>

  <rect x="550" y="350" width="210" height="50" rx="15" ry="15" class="db-thread" />
  <text x="655" y="380" class="text" text-anchor="middle">MariaDB</text>

  <!-- Queue -->
  <rect x="370" y="300" width="200" height="60" class="queue" />
  <text x="470" y="335" class="text" text-anchor="middle">DB Queue (queue.Queue)</text>

  <!-- Interactions -->

  <!-- Signals (Workers -> Main) -->
  <path d="M 290 75 L 240 85" class="signal-arrow" />
  <path d="M 290 145 L 240 155" class="signal-arrow" />
  <path d="M 290 215 L 240 225" class="signal-arrow" />
  <text x="265" y="175" class="text" text-anchor="middle">GUI Signals</text>

  <!-- Main updates UI -->
  <path d="M 135 270 L 135 350" class="arrow" />
  <text x="140" y="310" class="text">Renders Graph</text>

  <!-- Workers to Queue -->
  <path d="M 395 100 L 420 300" class="arrow" />
  <path d="M 395 170 L 440 300" class="arrow" />
  <path d="M 395 240 L 460 300" class="arrow" />
  <text x="370" y="270" class="text">Enqueue Data</text>

  <!-- Queue to DB Worker -->
  <path d="M 570 300 L 655 270" class="arrow" />
  <text x="600" y="290" class="text">Pulls Batch</text>

  <!-- DB Worker to DB -->
  <path d="M 655 270 L 655 350" class="arrow" />
  <text x="660" y="310" class="text">Batch Insert</text>

</svg>
```
<img width="805" height="500" alt="image" src="https://github.com/user-attachments/assets/be213dec-d94b-4446-b8c0-10a21d83a356" />


## 하드웨어 요구사항

본 코드는 아래의 하드웨어를 기준으로 작성되었으나, 설정 변경을 통해 유사한 장비에 적용 가능합니다.

- **데이터 수집 장비 (DAQ)**: National Instruments **cDAQ** 섀시 및 모듈
  - 온도 모듈 (RTD 입력용), 전압/전류 모듈 (아날로그 출력 센서용)
- **초음파 거리 센서**: **SICK UM30-213 계열** 또는 유사한 아날로그 출력 센서
- **라돈 검출기**: **RS9A** 또는 UART 시리얼 통신 지원 모델
- **자기장 센서**: **TFM1186** 또는 NI-VISA 지원 모델

## 소프트웨어 준비 및 설치

### 1. 필수 드라이버 설치

- **NI-DAQmx**: National Instruments 홈페이지에서 최신 드라이버 설치.
- **NI-VISA**: VISA 통신(자기장 센서 등)을 위해 필요.

### 2. Python 환경 설정 (Python 3.8+)

가상 환경 사용을 권장합니다.

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows
```

### 3. 필요 라이브러리 설치

```bash
pip install numpy pyqt5 pyqtgraph nidaqmx pyserial pyvisa mariadb
```

## 설정 방법 (`config_scm.json`)

프로그램 설정은 코드 수정 없이 외부 설정 파일 `config_scm.json`을 통해 관리됩니다. 프로그램 실행 전, 이 파일을 사용자의 환경에 맞게 수정해야 합니다.

```json
{
    "daq": {
        "rtd": {"device": "cDAQMod2", "channels": ["ai0", "ai1", "ai2"]},
        "volt": {
            "device": "cDAQMod1",
            "channels": ["ai0", "ai1"],
            "mapping": [
                // 채널 0 (ai0): 0~10V를 400~1800mm로 매핑
                {"volt_range": [0.0, 10.0], "dist_range_mm": [400, 1800]},
                // 채널 1 (ai1) 설정
                {"volt_range": [0.0, 10.0], "dist_range_mm": [400, 1800]}
            ]
        },
        "sampling_rate": 1000
    },
    "radon": {
        "port": "/dev/ttyUSB0", // Windows는 'COM3' 등
        // ...
    },
    "magnetometer": {
        "resource_name": "USB0::...",
        // ...
    },
    "database": {
        "enabled": false, // DB 사용 여부
        // ...
    }
}
```

- **`device` / `channels`**: NI-MAX 등에서 확인한 DAQ 모듈 이름과 채널 목록을 입력합니다.
- **`mapping`**: **(중요)** 초음파 센서의 `dist_range_mm` 값이 센서에 **물리적으로 티칭(Teach-in)한** 최소/최대 거리와 반드시 일치해야 합니다.
- **`port` / `resource_name`**: 시리얼 및 VISA 장비의 연결 정보를 입력합니다.
- **`database.enabled`**: DB 저장 기능을 활성화/비활성화합니다.

## 실행 방법

설정 파일(`config_scm.json`)과 실행 파일(`RENE_SCM.py`)이 동일한 디렉토리에 위치한 상태에서 아래 명령어를 실행합니다.

```bash
python RENE_SCM.py
```

프로그램은 시작 시 자동으로 하드웨어 연결 상태를 점검하며, 로그는 콘솔과 `scm_monitoring.log` 파일에 동시에 기록됩니다. `Ctrl+C`를 누르거나 창을 닫으면 모든 스레드가 안전하게 종료됩니다.

## 핵심 개념 해설

### 생산자-소비자 패턴 (Producer-Consumer Pattern)

이전 버전에서는 센서 데이터 수집 스레드가 DB 저장까지 담당하거나, GUI 스레드에서 DB 저장을 시도하여 병목 현상이 발생했습니다.

최적화된 버전에서는 이 문제를 해결하기 위해 생산자-소비자 패턴을 도입했습니다.

- **생산자 (Producers)**: `DaqWorker`, `RadonWorker` 등은 데이터를 생성(수집)하는 역할만 담당합니다. 생성된 데이터는 중앙의 안전한 `Queue`에 넣습니다.
- **소비자 (Consumer)**: `DatabaseWorker`는 `Queue`를 감시하다가 데이터가 쌓이면 이를 가져와 처리(DB 저장)합니다.
- **배치 처리**: DB 워커는 데이터를 즉시 삽입하지 않고 일정량 모아 **일괄 삽입(Batch Insert)**하여 DB 트랜잭션 효율을 극대화합니다.

이 구조는 각 역할이 서로에게 영향을 주지 않고 독립적으로 동작하게 하여 시스템 전체의 효율성과 안정성을 극대화합니다.

### NI-DAQ: 고속 샘플링과 평균화 기법 (Oversampling & Averaging)

노이즈가 많거나 응답 속도가 느린 센서(예: 초음파 센서)의 신호를 안정화하기 위한 기법입니다.

1.  **고속 샘플링(Oversampling)**: DAQ가 1초에 1000번(`1kHz`)의 데이터를 매우 빠르게 읽어들입니다.
2.  **평균화(Averaging)**: 1초 동안 수집된 1000개 샘플의 **평균**을 계산합니다.
3.  **결과**: 랜덤 노이즈는 상쇄되고, 1초를 대표하는 매우 안정적이고 정밀한 값을 얻을 수 있습니다.
