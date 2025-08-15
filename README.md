
# SCM - 통합 물리 센서 실시간 모터링 시스템 (Optimized)

## 개요

이 애플리케이션은 다양한 물리 센서(온도, 초음파 거리, 라돈, 자기장) 데이터를 실시간으로 수집, 처리, 시각화 및 저장하는 고성능 통합 모니터링 솔루션입니다.

최근 대규모 아키텍처 최적화를 통해, 시스템의 안정성과 성능이 극대화되었습니다. 모든 하드웨어 제어와 데이터베이스 작업은 GUI와 완전히 독립된 백그라운드 스레드에서 비동기적으로 처리되어, 장기간 운영 시에도 최고의 응답성과 안정성을 보장합니다.

-----

## 주요 개선 사항 및 특징

  - **고성능 비동기 아키텍처 (Optimized Architecture)**:

      - **생산자-소비자 패턴**: 센서 워커(생산자)와 데이터베이스 워커(소비자)를 분리하고 중앙 큐(`Queue`)로 연결하여, 데이터 수집과 DB 저장 부하를 완전히 분산시켰습니다.
      - **GUI 응답성 보장**: 모든 I/O 작업(하드웨어 통신, DB)이 백그라운드에서 처리되어 GUI 멈춤 현상이 발생하지 않습니다.

  - **다중 센서 통합 및 안정성**:

      - NI-DAQ (온도, 거리), 시리얼 (라돈), VISA (자기장) 등 다양한 인터페이스의 센서를 동시에 지원합니다.
      - **안정성이 검증된 워커 로직**: 하드웨어 특성에 맞춰 \*\*이벤트 기반(`QTimer`)\*\*과 **루프 기반(`while True`)** 워커를 혼용하여 각 센서의 통신 안정성을 극대화했습니다. (예: `MagnetometerWorker`)

  - **실시간 시각화 및 데이터 처리**:

      - **고속 그래프 렌더링**: `pyqtgraph`를 활용하여 대량의 데이터도 끊김 없이 시각화합니다.
      - **효율적인 데이터 버퍼**: 시계열 데이터 저장에 `collections.deque`를 사용하여 데이터 추가/삭제 성능을 O(1)로 최적화했습니다.

  - **오류 복구 및 사용자 피드백**:

      - **자동 재시도**: NI-DAQ 수집 중 일시적인 오류 발생 시 자동으로 연결을 재시도합니다.
      - **상세한 상태 알림**: 라돈 센서 안정화 과정의 **카운트다운**을 GUI에 표시하여 사용자에게 명확한 피드백을 제공합니다.

  - **최적화된 데이터베이스 연동**:

      - 전용 `DatabaseWorker` 스레드에서 \*\*배치 처리(Batch Processing)\*\*를 수행하여 DB 쓰기 부하를 최소화하고 삽입 효율을 극대화합니다.

  - **유지보수성 및 확장성**:

      - **설정 파일 분리**: 모든 환경 설정을 `config_scm.json`으로 외부화하여 코드 수정 없이 환경 변경이 가능합니다.
      - **구조화된 로깅**: 표준 `logging` 모듈을 도입하여 시스템 운영 기록 및 디버깅을 체계화했습니다.

-----

## 시스템 아키텍처

시스템은 크게 세 그룹의 스레드로 구성되며, 데이터는 \*\*시그널(Signal)\*\*과 \*\*큐(Queue)\*\*를 통해 스레드 간에 안전하게 전달됩니다.

1.  **메인 스레드 (GUI)**: 사용자 인터페이스 렌더링 및 이벤트 처리를 전담합니다.
2.  **워커 스레드 (Sensors - 생산자)**: 각 센서 하드웨어와의 통신 및 데이터 수집을 담당합니다. 수집된 데이터는 GUI 표시를 위해 메인 스레드로 **시그널**을 보내고, 영구 저장을 위해 DB 큐에 데이터를 넣습니다.
3.  **데이터베이스 스레드 (DB Worker - 소비자)**: DB 큐를 감시하다가 데이터가 쌓이면 이를 가져와 일괄 처리(Batch)하여 데이터베이스에 저장합니다.

<!-- end list -->

```svg
<svg width="800" height="500" viewBox="0 0 800 500" xmlns="http://www.w3.org/2000/svg">
  <style>
    .boundary { fill: none; stroke: #999; stroke-width: 1; stroke-dasharray: 5,5; }
    .main-thread { fill: #E3F2FD; stroke: #2196F3; stroke-width: 2; rx: 8;}
    .worker-thread { fill: #FFF3E0; stroke: #FF9800; stroke-width: 2; rx: 8;}
    .db-thread { fill: #E8F5E9; stroke: #4CAF50; stroke-width: 2; rx: 8;}
    .queue { fill: #F3E5F5; stroke: #9C27B0; stroke-width: 2; rx: 8;}
    .text { font-family: Arial, sans-serif; font-size: 12px; }
    .title { font-size: 14px; font-weight: bold; }
    .arrow { stroke: #333; stroke-width: 1.5; marker-end: url(#arrowhead); }
    .signal-arrow { stroke: #2196F3; stroke-width: 1.5; stroke-dasharray: 4,4; marker-end: url(#arrowhead-signal); }
  </style>

  <defs>
    <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="10" refY="3.5" orient="auto">
      <polygon points="0 0, 10 3.5, 0 7" fill="#333" />
    </marker>
    <marker id="arrowhead-signal" markerWidth="10" markerHeight="7" refX="10" refY="3.5" orient="auto">
      <polygon points="0 0, 10 3.5, 0 7" fill="#2196F3" />
    </marker>
  </defs>

    <rect x="10" y="10" width="250" height="480" class="boundary" />
  <text x="135" y="35" class="title" text-anchor="middle">메인 스레드 (GUI)</text>
  <rect x="270" y="10" width="250" height="480" class="boundary" />
  <text x="395" y="35" class="title" text-anchor="middle">워커 스레드 (생산자)</text>
  <rect x="530" y="10" width="260" height="480" class="boundary" />
  <text x="660" y="35" class="title" text-anchor="middle">DB 스레드 (소비자)</text>

    <rect x="30" y="200" width="210" height="100" class="main-thread" />
  <text x="135" y="245" class="text" text-anchor="middle">MainWindow</text>
  <text x="135" y="260" class="text" text-anchor="middle">(UI / Graph 렌더링)</text>

  <rect x="290" y="50" width="210" height="50" class="worker-thread" />
  <text x="395" y="75" class="text" text-anchor="middle">DaqWorker (Loop)</text>
  <rect x="290" y="120" width="210" height="50" class="worker-thread" />
  <text x="395" y="145" class="text" text-anchor="middle">RadonWorker (QTimer)</text>
  <rect x="290" y="190" width="210" height="50" class="worker-thread" />
  <text x="395" y="215" class="text" text-anchor="middle">MagnetometerWorker (Loop)</text>

  <rect x="550" y="220" width="210" height="60" class="db-thread" />
  <text x="655" y="255" class="text" text-anchor="middle">DatabaseWorker</text>
  <text x="655" y="270" class="text" text-anchor="middle">(Batch Processing)</text>
  <rect x="550" y="380" width="210" height="50" class="db-thread" />
  <text x="655" y="410" class="text" text-anchor="middle">MariaDB</text>

  <rect x="320" y="300" width="200" height="60" class="queue" />
  <text x="420" y="335" class="text" text-anchor="middle">DB Queue (thread-safe)</text>
  
    <path d="M 290 215 L 240 240" class="signal-arrow" />
  <path d="M 290 145 L 240 245" class="signal-arrow" />
  <path d="M 290 75 L 240 250" class="signal-arrow" />
  <text x="260" y="180" class="text" fill="#2196F3">PyQt Signal</text>
  <text x="260" y="195" class="text" fill="#2196F3">(실시간 UI 업데이트)</text>
  
  <path d="M 395 100 L 420 300" class="arrow" />
  <path d="M 395 170 L 420 300" class="arrow" />
  <path d="M 395 240 L 420 300" class="arrow" />
  <text x="355" y="270" class="text">Enqueue Data</text>

  <path d="M 520 330 L 590 280" class="arrow" />
  <text x="540" y="300" class="text">Dequeue Data</text>

  <path d="M 655 280 L 655 380" class="arrow" />
  <text x="660" y="330" class="text">Batch Insert</text>
</svg>
```
<img width="799" height="493" alt="image" src="https://github.com/user-attachments/assets/12de126f-2f07-45ab-846d-c2fb66824f39" />

-----

## 하드웨어 요구사항

본 코드는 아래 하드웨어를 기준으로 작성되었으나, 설정 변경을 통해 유사 장비에 적용 가능합니다.

  - **데이터 수집 장비 (DAQ)**: National Instruments **cDAQ** 섀시 및 모듈
      - 온도 모듈 (RTD 입력용), 전압 모듈 (아날로그 출력 센서용)
  - **초음파 거리 센서**: **SICK UM30 계열** 또는 0-10V 아날로그 출력 센서
  - **라돈 검출기**: **Ftlable(라돈아이)** 또는 호환 시리얼 통신 모델
  - **자기장 센서**: **Lakeshore TFM1186** 또는 NI-VISA 호환 SCPI 명령어 지원 모델

-----

## 소프트웨어 준비 및 설치

### 1\. 필수 드라이버 설치

  - **NI-DAQmx**: National Instruments 홈페이지에서 최신 드라이버 설치.
  - **NI-VISA**: VISA 통신(자기장 센서 등)을 위해 필요.

### 2\. Python 환경 설정 (Python 3.8+)

가상 환경(`venv`) 사용을 강력히 권장합니다.

```bash
# 프로젝트 디렉토리에서
python -m venv venv
source venv/bin/activate  # Linux/macOS
.\\venv\\Scripts\\activate  # Windows
```

### 3\. 필요 라이브러리 설치

```bash
pip install numpy pyqt5 pyqtgraph nidaqmx pyserial pyvisa mariadb
```

-----

## 설정 방법 (`config_scm.json`)

프로그램의 모든 설정은 외부 설정 파일 **`config_scm.json`** 을 통해 관리됩니다. 프로그램 실행 전, 이 파일을 사용자의 환경에 맞게 반드시 수정해야 합니다.

```json
{
    "logging_level": "INFO",
    "gui": {
        "max_data_points_days": 7
    },
    "daq": {
        "rtd": {"device": "cDAQ9189-2189707Mod2", "channels": ["ai0", "ai1", "ai2"]},
        "volt": {
            "device": "cDAQ9189-2189707Mod1",
            "channels": ["ai0", "ai1"],
            "mapping": [
                {"volt_range": [0.0, 10.0], "dist_range_mm": [400, 1800]},
                {"volt_range": [0.0, 10.0], "dist_range_mm": [400, 1800]}
            ]
        },
        "sampling_rate": 1000
    },
    "radon": {
        "port": "/dev/ttyUSB0",
        "interval_s": 600,
        "stabilization_s": 600
    },
    "magnetometer": {
        "resource_name": "USB0::0x1BFA::0x0498::0003055::INSTR",
        "library_path": "",
        "interval_s": 1.0
    },
    "database": {
        "enabled": false,
        "host": "127.0.0.1",
        "port": 3306,
        "user": "user",
        "password": "password",
        "database": "scm_data",
        "pool_size": 5
    }
}
```

  - **`device` / `channels`**: NI-MAX에서 확인한 DAQ 모듈 이름과 채널 목록을 정확히 입력합니다.
  - **`mapping`**: **(중요)** 초음파 센서의 `dist_range_mm` 값은 센서에 **물리적으로 티칭(Teach-in)한** 최소/최대 거리와 반드시 일치해야 합니다.
  - **`port` / `resource_name`**: 장치 관리자(Windows)나 `lsusb` (Linux) 명령으로 확인한 장비 연결 정보를 입력합니다.
  - **`database.enabled`**: DB 저장 기능을 활성화하려면 `true`로 변경하고 하단 정보를 기입합니다.

-----

## 실행 방법

설정 파일(`config_scm.json`)과 실행 파일이 동일한 디렉토리에 위치한 상태에서 아래 명령어를 실행합니다.

```bash
python RENE_SCM.py
```

프로그램은 시작 시 자동으로 하드웨어 연결 상태를 점검하며, 로그는 콘솔과 `scm_monitoring.log` 파일에 동시에 기록됩니다. `Ctrl+C`를 누르거나 창을 닫으면 모든 스레드가 안전하게 종료됩니다.

-----

## 핵심 개념 해설

### 생산자-소비자 패턴 (Producer-Consumer Pattern)

이전 버전은 센서 스레드가 DB 저장까지 담당하여 병목 현상이 발생할 수 있었습니다. 최적화된 아키텍처는 이를 **생산자-소비자 패턴**으로 해결합니다.

  - **생산자 (Producers)**: 각 센서 워커는 데이터를 \*\*생산(수집)\*\*하는 역할만 집중하고, 결과를 중앙의 안전한 저장소(`Queue`)에 넣습니다.
  - **소비자 (Consumer)**: `DatabaseWorker`는 `Queue`를 감시하다가 데이터가 쌓이면 이를 \*\*소비(DB 저장)\*\*합니다.
  - **배치 처리**: 소비자는 데이터를 즉시 저장하지 않고 일정량 모아 \*\*일괄 삽입(Batch Insert)\*\*하여 DB 트랜잭션 효율을 극대화합니다.

이 구조는 각 역할이 서로의 작업 속도에 영향을 받지 않고 독립적으로 동작하게 하여 시스템 전체의 처리량과 안정성을 극대화합니다.

### NI-DAQ: 오버샘플링 & 평균화 (Oversampling & Averaging)

노이즈가 많거나 응답 속도가 느린 센서(예: 초음파 센서)의 신호를 안정화하기 위한 기법입니다.

1.  **고속 샘플링(Oversampling)**: DAQ가 1초에 1000번(`1kHz`)의 데이터를 매우 빠르게 읽습니다.
2.  **평균화(Averaging)**: 1초 동안 수집된 1000개 샘플의 **평균**을 계산합니다.
3.  **결과**: 통계적으로 랜덤 노이즈는 0에 가깝게 상쇄되고, 1초를 대표하는 매우 안정적이고 정밀한 값을 얻을 수 있습니다.
