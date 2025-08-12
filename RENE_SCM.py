# -*- coding: utf-8 -*-
# ====================================================================================
# 통합 센서 실시간 모니터링 애플리케이션
# 최종 수정일: 2025-08-12
#
# 작성자: [최지영], [전남대학교 물리학과;전남대학교 중성미자 정밀연구센터]
#
# --- 개요 ---
# 이 애플리케이션은 다양한 종류의 물리 센서(온도, 초음파 거리, 라돈, 자기장)로부터
# 데이터를 실시간으로 수집, 처리, 시각화 및 저장하는 통합 모니터링 솔루션입니다.
# 각 하드웨어는 독립적인 백그라운드 스레드에서 제어되어 GUI의 응답성을 보장합니다.
#
# --- 사용된 핵심 기술 ---
# - GUI: PyQt5
# - 그래프: pyqtgraph
# - 데이터 수집: NI-DAQmx, PyVISA, PySerial
# - 데이터 처리: NumPy, SciPy
# - 데이터베이스: MariaDB (비활성화 상태)
# - 병렬 처리: QThread (멀티스레딩)
# ====================================================================================

# --- 필수 라이브러리 임포트 ---
import sys
import time
import numpy as np
import os
import math
import signal

# --- GUI 및 그래프 관련 ---
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QGridLayout, QMessageBox, QLabel, QVBoxLayout, QFrame
from PyQt5.QtCore import QThread, QObject, pyqtSignal, pyqtSlot, Qt
from PyQt5.QtGui import QFont
import pyqtgraph as pg

# --- 하드웨어 제어 관련 ---
import nidaqmx
from nidaqmx.constants import RTDType, ResistanceConfiguration, TerminalConfiguration, ExcitationSource, AcquisitionType
import serial
import pyvisa

# --- 데이터베이스 관련 ---
# MariaDB 연동을 위해 'pip install mariadb' 로 라이브러리 설치가 필요합니다.
# 실제 운용 시 아래 줄의 주석을 해제하십시오.
# import mariadb

# ====================================================================================
# NI-DAQmx 데이터 수집 워커 클래스
# ====================================================================================
class DaqWorker(QObject):
    """
    NI-DAQ 장비로부터 데이터를 수집하는 백그라운드 워커.
    - 온도(RTD) 및 전압(초음파 센서) 채널 동시 수집.
    - 고속 샘플링 후 평균화(Oversampling & Averaging) 기법으로 노이즈 제거.
    - 초음파 센서의 전압 값을 mm 단위 거리로 변환.
    """
    # GUI로 데이터를 전송하기 위한 시그널 정의
    avg_data_ready = pyqtSignal(float, list, list) # 1분 평균 데이터: timestamp, avg_rtd_list, avg_dist_list
    raw_data_ready = pyqtSignal(list, list)       # 1초 평균 실시간 데이터: raw_rtd_list, raw_dist_list
    error_occurred = pyqtSignal(str)              # 에러 메시지
    status_update = pyqtSignal(str)               # 상태바 메시지

    def __init__(self, daq_config):
        super().__init__()
        self._is_running = True
        self.config = daq_config
        self.num_rtd_channels = len(self.config['rtd']['channels'])
        self.num_volt_channels = len(self.config['volt']['channels'])
        
        # 1분 평균 계산을 위한 임시 데이터 버퍼
        self.temp_rtd_samples = [[] for _ in range(self.num_rtd_channels)]
        self.temp_dist_samples = [[] for _ in range(self.num_volt_channels)]

    def convert_voltage_to_distance(self, voltage, volt_channel_index):
        """
        초음파 센서에서 출력된 0-10V 전압을 mm 단위 거리로 변환합니다.
        선형 보간(Linear Interpolation) 공식을 사용합니다.
        
        Args:
            voltage (float): 측정된 전압 값.
            volt_channel_index (int): 설정(CONFIG)에 정의된 전압 채널의 인덱스.
        
        Returns:
            float: 변환된 거리 값 (mm).
        """
        # CONFIG에서 해당 전압 채널의 티칭된 매핑 정보 가져오기
        mapping_info = self.config['volt']['mapping'][volt_channel_index]
        v_min, v_max = mapping_info['volt_range']
        d_min, d_max = mapping_info['dist_range_mm']
        
        # 선형 보간 공식: Y = Y1 + ( (X - X1) / (X2 - X1) ) * (Y2 - Y1)
        distance = d_min + ((voltage - v_min) / (v_max - v_min)) * (d_max - d_min)
        
        # 변환된 거리가 설정된 범위를 벗어나지 않도록 값을 제한 (np.clip)
        return np.clip(distance, min(d_min, d_max), max(d_min, d_max))

    def run(self):
        """워커 스레드의 메인 실행 루프."""
        try:
            self.status_update.emit("NI-DAQ Task 생성 중...")
            with nidaqmx.Task() as task:
                self.status_update.emit("NI-DAQ 채널 설정 중...")
                
                # RTD(Pt100) 온도 센서 채널 추가
                rtd_device = self.config['rtd']['device']
                rtd_ch_list = [f"{rtd_device}/{ch}" for ch in self.config['rtd']['channels']]
                task.ai_channels.add_ai_rtd_chan(','.join(rtd_ch_list), rtd_type=RTDType.PT_3851, resistance_config=ResistanceConfiguration.THREE_WIRE, current_excit_source=ExcitationSource.INTERNAL, current_excit_val=0.001)
                
                # 전압(초음파 센서) 채널 추가
                volt_device = self.config['volt']['device']
                volt_ch_list = [f"{volt_device}/{ch}" for ch in self.config['volt']['channels']]
                task.ai_channels.add_ai_voltage_chan(','.join(volt_ch_list), min_val=0.0, max_val=10.0, terminal_config=TerminalConfiguration.DEFAULT)

                # 샘플링 클락 설정: 고속 샘플링(Oversampling)
                # SICK 센서의 낮은 응답속도(8Hz)와 전기적 노이즈를 극복하기 위해
                # DAQ는 1초에 1000개의 데이터를 매우 빠르게 수집합니다.
                sampling_rate = 1000  # 1kHz
                samples_to_read_per_second = sampling_rate
                task.timing.cfg_samp_clk_timing(rate=sampling_rate, sample_mode=AcquisitionType.CONTINUOUS, samps_per_chan=samples_to_read_per_second)
                
                self.status_update.emit("NI-DAQ 측정 시작됨")
                task.start()
                
                while self._is_running:
                    # 1초 동안 1000개의 샘플 덩어리(chunk)를 읽어들임
                    measured_data_chunk = task.read(number_of_samples_per_channel=samples_to_read_per_second)
                    
                    # 평균화(Averaging): 읽어들인 1000개 샘플의 평균을 계산하여 노이즈를 제거.
                    # 이 평균값이 1초를 대표하는 안정적인 '실시간 값'이 됨.
                    rtd_values = [np.mean(chunk) for chunk in measured_data_chunk[0:self.num_rtd_channels]]
                    voltage_values = [np.mean(chunk) for chunk in measured_data_chunk[self.num_rtd_channels:]]
                    
                    # 계산된 1초 평균 전압을 거리(mm)로 즉시 변환
                    dist_values_mm = [self.convert_voltage_to_distance(v, i) for i, v in enumerate(voltage_values)]

                    # GUI에 안정화된 '실시간 값'(온도와 거리)을 업데이트
                    self.raw_data_ready.emit(rtd_values, dist_values_mm)
                    
                    # 1분 평균 계산을 위해 1초 평균값을 버퍼에 저장
                    for i in range(self.num_rtd_channels): self.temp_rtd_samples[i].append(rtd_values[i])
                    for i in range(self.num_volt_channels): self.temp_dist_samples[i].append(dist_values_mm[i])
                    
                    # 60개의 1초 평균값이 모이면 (즉, 1분이 지나면) 1분 평균을 계산
                    if self.temp_rtd_samples and len(self.temp_rtd_samples[0]) >= 60:
                        timestamp = time.time()
                        
                        avg_rtd = [np.mean(ch_data) for ch_data in self.temp_rtd_samples]
                        avg_dist = [np.mean(ch_data) for ch_data in self.temp_dist_samples]

                        # 1분 평균값을 GUI로 전송
                        self.avg_data_ready.emit(timestamp, avg_rtd, avg_dist)
                        
                        # 버퍼 초기화
                        self.temp_rtd_samples = [[] for _ in range(self.num_rtd_channels)]
                        self.temp_dist_samples = [[] for _ in range(self.num_volt_channels)]
                        
        except nidaqmx.errors.DaqError as e:
            self.error_occurred.emit(f"NI-DAQ 오류: {e}")
        except Exception as e:
            self.error_occurred.emit(f"NI-DAQ 워커에서 알 수 없는 오류 발생: {e}")
        finally:
            print("DAQ 워커 스레드가 종료되었습니다.")

    def stop(self):
        """스레드 종료를 위한 플래그 설정."""
        print("DAQ 워커 중지 요청 수신...")
        self._is_running = False

# ====================================================================================
# 라돈 측정기(FRD400) 데이터 수집 워커 클래스
# ====================================================================================
class RadonWorker(QObject):
    """시리얼(UART) 통신으로 라돈 측정기 데이터를 수집하는 백그라운드 워커."""
    data_ready = pyqtSignal(float, float, float)
    status_update = pyqtSignal(str)
    error_occurred = pyqtSignal(str)

    def __init__(self, port):
        super().__init__()
        self._is_running = True
        self.port = port

    def run(self):
        ser_radon = None
        try:
            self.status_update.emit("라돈 센서 연결 중...")
            # 시리얼 포트 설정: 19200 보드레이트, 패리티 없음, 타임아웃 10초
            ser_radon = serial.Serial(self.port, 19200, parity=serial.PARITY_NONE, timeout=10)
            
            # 장비 초기화 및 설정 명령어 전송
            self.status_update.emit("라돈 센서 초기화 중..."); ser_radon.write(b'RESET\r\n'); ser_radon.readline()
            ser_radon.write(b'UNIT 1\r\n'); ser_radon.readline() # Bq/m^3 단위로 설정
            
            # FRD400은 초기 이온화 및 안정화에 시간이 필요 (매뉴얼 기준 10분)
            self.status_update.emit("라돈 센서 이온화 대기 중 (10분)...")
            for i in range(600):
                if not self._is_running: break
                time.sleep(1)
                if i % 10 == 0: self.status_update.emit(f"라돈 센서 이온화 대기 중... ({600-i}초 남음)")
            
            if self._is_running: self.status_update.emit("라돈 측정 시작됨")

            while self._is_running:
                start_time = time.time()
                ser_radon.write(b'VALUE?\r\n') # 데이터 요청 명령어
                try:
                    response = ser_radon.readline().decode('ascii')
                    # 수신된 문자열 파싱하여 mu(평균), sigma(표준편차) 값 추출
                    mu = float(response.split(':')[1].split(' ')[1])
                    sigma = float(response.split(':')[2].split(' ')[1])
                    self.data_ready.emit(time.time(), mu, sigma)
                except (IndexError, ValueError) as e: 
                    self.error_occurred.emit(f"라돈 데이터 파싱 오류: {e}, 받은 값: {response}")
                
                # 10분(600초) 간격으로 측정
                wait_time = 600 - (time.time() - start_time)
                for _ in range(int(wait_time)):
                    if not self._is_running: break
                    time.sleep(1)

        except serial.SerialException as e:
            self.error_occurred.emit(f"Radon 시리얼 포트 오류: {e}")
        finally:
            if ser_radon and ser_radon.is_open: ser_radon.close()
            print("라돈 워커 스레드가 종료되었습니다.")
            
    def stop(self): self._is_running = False

# ====================================================================================
# 자기장 센서(Lakeshore TFM1186) 데이터 수집 워커 클래스
# ====================================================================================
class MagnetometerWorker(QObject):
    """VISA 통신으로 3축 자기장 센서 데이터를 수집하는 백그라운드 워커."""
    avg_data_ready = pyqtSignal(float, list) 
    raw_data_ready = pyqtSignal(list)
    error_occurred = pyqtSignal(str)
    status_update = pyqtSignal(str)

    def __init__(self, resource_name, library_path, interval):
        super().__init__()
        self._is_running = True
        self.resource_name = resource_name
        self.library_path = library_path
        self.interval = interval # 측정 간격 (초)
        self.mag_samples = [[] for _ in range(4)] # Bx, By, Bz, |B|

    def run(self):
        instrument = None
        try:
            self.status_update.emit("자기장 센서 연결 중...")
            # VISA 리소스 매니저 초기화
            rm = pyvisa.ResourceManager(self.library_path)
            instrument = rm.open_resource(self.resource_name, timeout=5000)
            instrument.read_termination = '\n'; instrument.write_termination = '\n'
            
            self.status_update.emit("자기장 센서 설정 중...")
            # SCPI(Standard Commands for Programmable Instruments) 명령어 전송
            instrument.write('*RST'); time.sleep(1.5) # 장비 리셋
            instrument.write('*CLS'); time.sleep(0.2) # 상태 레지스터 클리어
            instrument.write('SENSE:RANGE:AUTO ON'); time.sleep(0.2) # 측정 범위 자동
            instrument.write('UNIT MGAUSS'); time.sleep(0.2) # 단위 mGauss로 설정
            instrument.write('TRIGger:SOURce TIMer'); time.sleep(0.2) # 트리거 소스를 내부 타이머로
            instrument.write(f'TRIGger:TIMer {self.interval}'); time.sleep(0.2) # 트리거 간격 설정
            instrument.write('INITiate:CONTinuous ON'); time.sleep(0.2) # 연속 측정 모드 활성화
            instrument.write('INITiate'); time.sleep(self.interval * 2) # 측정 시작
            
            self.status_update.emit("자기장 측정 시작됨")
            
            while self._is_running:
                # FETCh 명령으로 X, Y, Z축 데이터를 한 번에 쿼리
                response = instrument.query(':FETCh:SCALar:FLUX:X?;:FETCh:SCALar:FLUX:Y?;:FETCh:SCALar:FLUX:Z?')
                if response is None: continue
                
                parts = response.strip().split(';')
                if len(parts) == 3:
                    try:
                        bx = float(parts[0].strip().split(' ')[0])
                        by = float(parts[1].strip().split(' ')[0])
                        bz = float(parts[2].strip().split(' ')[0])
                        b_mag = math.sqrt(bx**2 + by**2 + bz**2) # 자기장 크기 계산
                        
                        raw_values = [bx, by, bz, b_mag]
                        self.raw_data_ready.emit(raw_values)
                        
                        for i in range(4): self.mag_samples[i].append(raw_values[i])

                        # 60개의 샘플이 모이면 1분 평균 계산
                        if len(self.mag_samples[0]) >= 60:
                            timestamp = time.time()
                            avg_mag = [np.mean(ch_data) for ch_data in self.mag_samples]
                            self.avg_data_ready.emit(timestamp, avg_mag)
                            self.mag_samples = [[] for _ in range(4)]
                    except (ValueError, IndexError):
                        pass
                
                time.sleep(self.interval)

        except pyvisa.errors.VisaIOError as e:
            self.error_occurred.emit(f"자기장 센서 VISA 오류: {e.description}")
        finally:
            if instrument:
                try: 
                    instrument.write('ABORt') # 측정 중지
                    instrument.close()
                except: pass
            print("자기장 워커 스레드가 종료되었습니다.")

    def stop(self): self._is_running = False

# ====================================================================================
# 메인 GUI 윈도우 클래스
# ====================================================================================
class MainWindow(QMainWindow):
    """애플리케이션의 메인 GUI 창. UI 요소 생성 및 데이터 업데이트 담당."""
    def __init__(self, config):
        super().__init__()
        self.setWindowTitle("통합 센서 실시간 모니터링")
        self.setGeometry(100, 100, 1600, 950)
        
        self.config = config
        self.is_daq_available = config['daq']['available']
        self.is_radon_available = config['radon']['available']
        self.is_mag_available = config['magnetometer']['available']
        self.db_pool = None

        # 메인 레이아웃 설정
        central_widget = QWidget(); self.setCentralWidget(central_widget)
        self.main_layout = QGridLayout(central_widget)
        plot_layout = QGridLayout()
        self.main_layout.addLayout(plot_layout, 0, 0, 1, 3) # 그래프 영역
        self.main_layout.addLayout(self.create_value_panel(), 0, 3, 1, 1) # 데이터 표시 패널

        # 그래프 위젯 생성
        self.plot_curves = {}
        plot_row_index = 0
        if self.is_daq_available:
            num_rtd = len(self.config['daq']['rtd']['channels'])
            num_volt = len(self.config['daq']['volt']['channels'])
            plot_layout.addWidget(self.create_plot_widget("온도 (°C) (1분 평균)", "°C", [f"RTD {i+1}" for i in range(num_rtd)], ['r', 'g', 'b']), plot_row_index, 0); plot_row_index += 1
            plot_layout.addWidget(self.create_plot_widget("초음파 거리 (mm) (1분 평균)", "mm", [f"거리 {i+1}" for i in range(num_volt)], ['c', 'm']), plot_row_index, 0); plot_row_index += 1
        
        if self.is_radon_available:
            plot_layout.addWidget(self.create_plot_widget("라돈 (Bq/m³) (10분 간격)", "Bq/m³", ["Radon (μ)"], ['#FF8C00']), plot_row_index, 0); plot_row_index += 1
        if self.is_mag_available:
            plot_layout.addWidget(self.create_plot_widget("자기장 (mG) (1분 평균)", "mG", ["Bx", "By", "Bz", "|B|"], ['#0000FF', '#008000', '#FF0000', '#000000']), plot_row_index, 0)
        
        # 그래프 데이터 저장을 위한 리스트
        self.max_points = 7 * 24 * 60 # 최대 7일치 1분 평균 데이터 저장
        self.time_data = []; self.rtd_data = [[] for _ in range(len(config['daq']['rtd']['channels']))]; self.dist_data = [[] for _ in range(len(config['daq']['volt']['channels']))]
        self.radon_time_data = []; self.radon_mu_data = []
        self.mag_time_data = []; self.mag_data = [[] for _ in range(4)]

        self.setup_database_connection()
        self.setup_threads()

    def create_value_panel(self):
        """GUI 우측에 위치하는 데이터 표시 패널을 생성합니다."""
        panel_layout = QVBoxLayout(); panel_layout.setAlignment(Qt.AlignTop)
        title_font = QFont("Arial", 16, QFont.Bold); value_font = QFont("Arial", 14)
        
        # 실시간 값 표시 영역
        if self.is_daq_available or self.is_mag_available:
            panel_layout.addWidget(QLabel("실시간 값 (1초 갱신)"), 0, Qt.AlignTop)
            self.raw_value_labels = {}
            if self.is_daq_available:
                for i in range(len(self.config['daq']['rtd']['channels'])): name = f"RTD {i+1}"; label = QLabel(f"{name}: -"); label.setFont(value_font); panel_layout.addWidget(label); self.raw_value_labels[name] = label
                for i in range(len(self.config['daq']['volt']['channels'])): name = f"거리 {i+1}"; label = QLabel(f"{name}: -"); label.setFont(value_font); panel_layout.addWidget(label); self.raw_value_labels[name] = label
            if self.is_mag_available:
                for name in ["B_x", "B_y", "B_z", "|B|"]: label = QLabel(f"{name}: -"); label.setFont(value_font); panel_layout.addWidget(label); self.raw_value_labels[name] = label
            panel_layout.addSpacing(10); panel_layout.addWidget(self.create_separator())

        # 1분 평균값 표시 영역
        if self.is_daq_available or self.is_mag_available:
            self.avg_title_label = QLabel("최신 측정값 (1분 평균)"); self.avg_title_label.setFont(title_font)
            panel_layout.addWidget(self.avg_title_label)
            self.avg_value_labels = {}
            if self.is_daq_available:
                for i in range(len(self.config['daq']['rtd']['channels'])): name = f"RTD {i+1}"; label = QLabel(f"{name}: -"); label.setFont(value_font); panel_layout.addWidget(label); self.avg_value_labels[name] = label
                for i in range(len(self.config['daq']['volt']['channels'])): name = f"거리 {i+1}"; label = QLabel(f"{name}: -"); label.setFont(value_font); panel_layout.addWidget(label); self.avg_value_labels[name] = label
            if self.is_mag_available:
                for name in ["B_x", "B_y", "B_z", "|B|"]: label = QLabel(f"{name}: -"); label.setFont(value_font); panel_layout.addWidget(label); self.avg_value_labels[name] = label
            panel_layout.addSpacing(20); panel_layout.addWidget(self.create_separator()); panel_layout.addSpacing(20)

        # 라돈 값 표시 영역
        if self.is_radon_available:
            self.radon_title_label = QLabel("라돈 측정값 (10분 갱신)"); self.radon_title_label.setFont(title_font)
            panel_layout.addWidget(self.radon_title_label)
            self.radon_value_label = QLabel("μ ± σ: -"); self.radon_value_label.setFont(value_font)
            panel_layout.addWidget(self.radon_value_label)
            self.radon_status_label = QLabel("상태: 대기 중..."); self.radon_status_label.setFont(value_font)
            panel_layout.addWidget(self.radon_status_label)
        return panel_layout

    def create_separator(self):
        """패널 내 구분을 위한 수평선을 생성합니다."""
        separator = QFrame(); separator.setFrameShape(QFrame.HLine); separator.setFrameShadow(QFrame.Sunken); return separator

    def create_plot_widget(self, title, y_label, legend_names, colors):
        """pyqtgraph 기반의 그래프 위젯을 생성합니다."""
        plot_widget = pg.PlotWidget(title=title)
        plot_widget.setBackground('w')
        plot_widget.addLegend()
        plot_widget.showGrid(x=True, y=True)
        plot_widget.setLabel('left', y_label)
        plot_widget.setLabel('bottom', '시간 (HH:MM:SS)')
        # X축을 시간 포맷으로 표시하기 위한 설정
        plot_widget.setAxisItems({'bottom': pg.DateAxisItem(orientation='bottom')})
        for i, name in enumerate(legend_names):
            self.plot_curves[f"{title}_{name}"] = plot_widget.plot(pen=pg.mkPen(colors[i], width=2), name=name)
        return plot_widget

    def setup_database_connection(self):
        """[DB] MariaDB 커넥션 풀을 생성하고 테이블의 존재 여부를 확인/생성합니다."""
        # # 실제 운용 시, 아래의 모든 주석을 해제하고 서버 관리자 명세에 맞게 수정해야 합니다.
        # self.db_pool = None
        # db_config = self.config['database']
        # try:
        #     print("데이터베이스 커넥션 풀을 생성합니다...")
        #     self.db_pool = mariadb.ConnectionPool(user=db_config['user'], password=db_config['password'], host=db_config['host'], port=db_config['port'], database=db_config['database'], pool_name="monitoring_pool", pool_size=5)
        #     
        #     print("데이터베이스 테이블을 확인/생성합니다...")
        #     conn = self.db_pool.get_connection()
        #     cursor = conn.cursor()
        #     
        #     # 테이블 생성 SQL: 전압(VOLT) 대신 거리(DIST)를 저장
        #     if self.is_daq_available:
        #         cursor.execute("CREATE TABLE IF NOT EXISTS SENSOR_DATA (datetime DATETIME PRIMARY KEY, RTD_1 DECIMAL(6,2), RTD_2 DECIMAL(6,2), RTD_3 DECIMAL(6,2), DIST_1 DECIMAL(7,1), DIST_2 DECIMAL(7,1))")
        #     if self.is_radon_available:
        #         cursor.execute("CREATE TABLE IF NOT EXISTS RADON_DATA (datetime DATETIME PRIMARY KEY, mu DECIMAL(6,2), sigma DECIMAL(6,2))")
        #     if self.is_mag_available:
        #         cursor.execute("CREATE TABLE IF NOT EXISTS MAGNETOMETER_DATA (datetime DATETIME PRIMARY KEY, Bx DECIMAL(8,2), By DECIMAL(8,2), Bz DECIMAL(8,2), B_mag DECIMAL(8,2))")
        #     
        #     conn.commit()
        #     print("데이터베이스 준비 완료.")
        #     
        # except mariadb.Error as e:
        #     self.show_error_message(f"MariaDB 연결 실패: {e}")
        #     self.db_pool = None
        # finally:
        #     if 'conn' in locals() and conn.is_connected(): conn.close()
        pass

    def setup_threads(self):
        """각 센서의 데이터 수집을 위한 백그라운드 스레드를 설정하고 시작합니다."""
        if self.is_daq_available:
            self.daq_thread = QThread()
            self.daq_worker = DaqWorker(self.config['daq'])
            self.daq_worker.moveToThread(self.daq_thread)
            self.daq_thread.started.connect(self.daq_worker.run)
            self.daq_worker.avg_data_ready.connect(self.update_averaged_ui)
            self.daq_worker.raw_data_ready.connect(self.update_raw_ui)
            self.daq_worker.error_occurred.connect(self.show_error_message)
            self.daq_worker.status_update.connect(self.show_status_message)
            self.daq_thread.start()
        
        if self.is_radon_available:
            self.radon_thread = QThread()
            self.radon_worker = RadonWorker(self.config['radon']['port'])
            self.radon_worker.moveToThread(self.radon_thread)
            self.radon_thread.started.connect(self.radon_worker.run)
            self.radon_worker.data_ready.connect(self.update_radon_ui)
            self.radon_worker.status_update.connect(self.update_radon_status)
            self.radon_worker.error_occurred.connect(self.show_error_message)
            self.radon_thread.start()
        
        if self.is_mag_available:
            mag_conf = self.config['magnetometer']
            self.mag_thread = QThread()
            self.mag_worker = MagnetometerWorker(mag_conf['resource_name'], mag_conf['library_path'], mag_conf['interval'])
            self.mag_worker.moveToThread(self.mag_thread)
            self.mag_thread.started.connect(self.mag_worker.run)
            self.mag_worker.avg_data_ready.connect(self.update_magnetometer_ui)
            self.mag_worker.raw_data_ready.connect(self.update_raw_magnetometer_ui)
            self.mag_worker.error_occurred.connect(self.show_error_message)
            self.mag_worker.status_update.connect(self.show_status_message)
            self.mag_thread.start()
    
    # 각 워커로부터 시그널을 받아 GUI를 업데이트하는 @pyqtSlot 데코레이터 메서드들
    @pyqtSlot(float, list, list)
    def update_averaged_ui(self, timestamp, avg_rtd, avg_dist):
        """1분 평균 NI-DAQ 데이터로 UI를 업데이트합니다."""
        self.time_data.append(timestamp)
        dt_obj = time.strftime('%H:%M:%S', time.localtime(timestamp))
        self.avg_title_label.setText(f"최신 측정값 (갱신: {dt_obj})")
        
        for i in range(len(self.config['daq']['rtd']['channels'])): 
            self.rtd_data[i].append(avg_rtd[i])
            self.avg_value_labels[f"RTD {i+1}"].setText(f"RTD {i+1}: {avg_rtd[i]:.2f} °C")
        
        for i in range(len(self.config['daq']['volt']['channels'])): 
            self.dist_data[i].append(avg_dist[i])
            self.avg_value_labels[f"거리 {i+1}"].setText(f"거리 {i+1}: {avg_dist[i]:.0f} mm")
        
        if len(self.time_data) > self.max_points: 
            self.time_data.pop(0); [d.pop(0) for d in self.rtd_data]; [d.pop(0) for d in self.dist_data]
        
        for i, n in enumerate([f"RTD {j+1}" for j in range(len(self.config['daq']['rtd']['channels']))]): 
            self.plot_curves[f"온도 (°C) (1분 평균)_{n}"].setData(x=self.time_data, y=self.rtd_data[i])
        for i, n in enumerate([f"거리 {j+1}" for j in range(len(self.config['daq']['volt']['channels']))]): 
            self.plot_curves[f"초음파 거리 (mm) (1분 평균)_{n}"].setData(x=self.time_data, y=self.dist_data[i])
            
        self.push_sensor_data_to_db(timestamp, avg_rtd, avg_dist)
        
    @pyqtSlot(list, list)
    def update_raw_ui(self, raw_rtd, raw_dist):
        """1초 평균 실시간 NI-DAQ 데이터로 UI를 업데이트합니다."""
        for i in range(len(self.config['daq']['rtd']['channels'])): 
            self.raw_value_labels[f"RTD {i+1}"].setText(f"RTD {i+1}: {raw_rtd[i]:.2f} °C")
        for i in range(len(self.config['daq']['volt']['channels'])): 
            self.raw_value_labels[f"거리 {i+1}"].setText(f"거리 {i+1}: {raw_dist[i]:.0f} mm")

    def push_sensor_data_to_db(self, timestamp, rtd_data, dist_data):
        """[DB] 1분 평균 NI-DAQ 데이터를 데이터베이스에 저장합니다."""
        # if not self.db_pool: return
        # try:
        #     conn = self.db_pool.get_connection(); cursor = conn.cursor()
        #     dt_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
        #     # SQL 구문에서 DIST_1, DIST_2에 거리 데이터를 저장
        #     sql = "INSERT IGNORE INTO SENSOR_DATA (datetime, RTD_1, RTD_2, RTD_3, DIST_1, DIST_2) VALUES (?, ?, ?, ?, ?, ?)"
        #     cursor.execute(sql, (dt_str, round(rtd_data[0], 2), round(rtd_data[1], 2), round(rtd_data[2], 2), round(dist_data[0], 1), round(dist_data[1], 1)))
        #     conn.commit()
        # except mariadb.Error as e: print(f"DB 저장 에러 (Sensor): {e}")
        # finally:
        #     if 'cursor' in locals() and cursor: cursor.close()
        #     if 'conn' in locals() and conn: conn.close()
        pass

    @pyqtSlot(float, list)
    def update_magnetometer_ui(self, timestamp, avg_mag):
        """1분 평균 자기장 데이터로 UI를 업데이트합니다."""
        self.mag_time_data.append(timestamp)
        for i in range(4): self.mag_data[i].append(avg_mag[i])
        if len(self.mag_time_data) > self.max_points: self.mag_time_data.pop(0); [d.pop(0) for d in self.mag_data]
        
        for i, n in enumerate(["Bx", "By", "Bz", "|B|"]): self.plot_curves[f"자기장 (mG) (1분 평균)_{n}"].setData(x=self.mag_time_data, y=self.mag_data[i])
        
        dt_obj = time.strftime('%H:%M:%S', time.localtime(timestamp))
        self.avg_title_label.setText(f"최신 측정값 (갱신: {dt_obj})")
        for i, n in enumerate(["B_x", "B_y", "B_z", "|B|"]): self.avg_value_labels[n].setText(f"{n}: {avg_mag[i]:.2f} mG")
        self.push_magnetometer_data_to_db(timestamp, avg_mag)
        
    @pyqtSlot(list)
    def update_raw_magnetometer_ui(self, raw_mag):
        """실시간 자기장 데이터로 UI를 업데이트합니다."""
        for i, n in enumerate(["B_x", "B_y", "B_z", "|B|"]): self.raw_value_labels[n].setText(f"{n}: {raw_mag[i]:.2f} mG")

    def push_magnetometer_data_to_db(self, timestamp, avg_mag):
        """[DB] 1분 평균 자기장 데이터를 데이터베이스에 저장합니다."""
        # if not self.db_pool: return
        # ... (DB 저장 로직)
        pass

    @pyqtSlot(float, float, float)
    def update_radon_ui(self, timestamp, mu, sigma):
        """10분 간격 라돈 데이터로 UI를 업데이트합니다."""
        self.radon_time_data.append(timestamp); self.radon_mu_data.append(mu)
        if len(self.radon_time_data) > (7 * 24 * 6): self.radon_time_data.pop(0); self.radon_mu_data.pop(0)
        self.plot_curves["라돈 (Bq/m³) (10분 간격)_Radon (μ)"].setData(x=self.radon_time_data, y=self.radon_mu_data)
        dt_obj = time.strftime('%H:%M:%S', time.localtime(timestamp))
        self.radon_title_label.setText(f"라돈 측정값 (갱신: {dt_obj})")
        self.radon_value_label.setText(f"μ ± σ: {mu:.2f} ± {sigma:.2f} (Bq/m³)")
        self.push_radon_data_to_db(timestamp, mu, sigma)

    def push_radon_data_to_db(self, timestamp, mu, sigma):
        """[DB] 10분 간격 라돈 데이터를 데이터베이스에 저장합니다."""
        # if not self.db_pool: return
        # ... (DB 저장 로직)
        pass

    @pyqtSlot(str)
    def update_radon_status(self, status):
        """라돈 센서의 상태 메시지를 UI에 업데이트합니다."""
        self.radon_status_label.setText(f"상태: {status}")

    @pyqtSlot(str)
    def show_status_message(self, message):
        """상태바에 메시지를 잠시 표시합니다."""
        self.statusBar().showMessage(message, 5000) # 5000ms = 5초간 표시

    @pyqtSlot(str)
    def show_error_message(self, message):
        """에러 발생 시 사용자에게 메시지 박스를 표시합니다."""
        QMessageBox.critical(self, "오류 발생", f"에러가 발생했습니다:\n\n{message}")

    def closeEvent(self, event):
        """GUI 창이 닫힐 때 모든 백그라운드 스레드를 안전하게 종료합니다."""
        print("애플리케이션을 종료합니다...")
        if hasattr(self, 'daq_thread') and self.daq_thread.isRunning(): self.daq_worker.stop(); self.daq_thread.quit(); self.daq_thread.wait()
        if hasattr(self, 'radon_thread') and self.radon_thread.isRunning(): self.radon_worker.stop(); self.radon_thread.quit(); self.radon_thread.wait()
        if hasattr(self, 'mag_thread') and self.mag_thread.isRunning(): self.mag_worker.stop(); self.mag_thread.quit(); self.mag_thread.wait()
        # if self.db_pool: self.db_pool.close() # DB 커넥션 풀 종료
        event.accept()

# ====================================================================================
# 프로그램 실행 진입점 (Entry Point)
# ====================================================================================
if __name__ == '__main__':
    # --- [사용자 설정 영역] ---
    # 이 부분에서 각 하드웨어의 연결 정보와 동작 여부를 설정합니다.
    CONFIG = {
        'daq': {
            'available': False, # NI-DAQ 연결 여부
            'rtd': {'device': 'cDAQ9189-2189707Mod2', 'channels': ['ai0', 'ai1', 'ai2']},
            'volt': {
                'device': 'cDAQ9189-2189707Mod1', 
                'channels': ['ai0', 'ai1'],
                # 각 전압 채널에 대한 전압-거리 매핑 정보
                # SICK UM30-213118 센서를 400mm~1800mm로 티칭하고 0~10V로 출력받는 경우
                'mapping': [
                    {'volt_range': [0.0, 10.0], 'dist_range_mm': [400, 1800]}, # ai0에 대한 설정
                    {'volt_range': [0.0, 10.0], 'dist_range_mm': [400, 1800]}  # ai1에 대한 설정
                ]
            }
        },
        'radon': {
            'available': False, # 라돈 센서 연결 여부
            'port': '/dev/ttyUSB0' # Linux 기준, Windows는 'COM3' 등
        },
        'magnetometer': {
            'available': False, # 자기장 센서 연결 여부
            'resource_name': 'USB0::0x1BFA::0x0498::0003055::INSTR',
            'library_path': '/usr/lib/x86_64-linux-gnu/libvisa.so', # Linux 기준, Windows는 비워두거나 NI-VISA 설치 경로 지정
            'interval': 1.0 # 1초 간격
        },
        'database': {
            'user': 'your_db_user',
            'password': 'your_db_password',
            'host': '127.0.0.1',
            'port': 3306,
            'database': 'your_db_name'
        }
    }
    
    # --- 애플리케이션 시작 ---
    print("="*50, "\n통합 센서 모니터링 시스템 시작\n" + "="*50)
    print("[설정 정보]")
    # ... (설정 정보 출력 로직)

    print("[하드웨어 감지]")
    # NI-DAQ 장비 감지
    try:
        system = nidaqmx.system.System.local()
        if system.devices:
            print(f"NI DAQ 장치가 연결되었습니다: {[dev.name for dev in system.devices]}")
            CONFIG['daq']['available'] = True
        else: 
            print("경고: 연결된 NI DAQ 장치가 없습니다.")
            CONFIG['daq']['available'] = False
    except Exception as e: 
        print(f"경고: NI-DAQmx 드라이버 또는 서비스 오류. ({e})")
        CONFIG['daq']['available'] = False
    
    # 라돈 센서 포트 감지 (Linux/Mac)
    if os.path.exists(CONFIG['radon']['port']): 
        print(f"라돈 측정기 포트({CONFIG['radon']['port']})가 확인되었습니다.")
        CONFIG['radon']['available'] = True
    else: 
        print(f"경고: 라돈 측정기 포트({CONFIG['radon']['port']})를 찾을 수 없습니다.")
        CONFIG['radon']['available'] = False

    # 자기장 센서 VISA 리소스 감지
    try:
        rm = pyvisa.ResourceManager(CONFIG['magnetometer']['library_path'])
        if CONFIG['magnetometer']['resource_name'] in rm.list_resources():
            print(f"자기장 센서({CONFIG['magnetometer']['resource_name']})가 연결되었습니다.")
            CONFIG['magnetometer']['available'] = True
        else: 
            print(f"경고: 자기장 센서({CONFIG['magnetometer']['resource_name']})를 찾을 수 없습니다.")
            CONFIG['magnetometer']['available'] = False
    except Exception as e: 
        print(f"경고: VISA 라이브러리(NI-VISA 등) 또는 서비스 오류. ({e})")
        CONFIG['magnetometer']['available'] = False

    # PyQt 애플리케이션 실행
    app = QApplication(sys.argv)
    
    # Ctrl+C로 안전하게 종료하기 위한 시그널 핸들러
    def sigint_handler(*args):
        print("\nCtrl+C 감지! 애플리케이션을 안전하게 종료합니다...")
        QApplication.quit()
    signal.signal(signal.SIGINT, sigint_handler)
    
    # 시그널 핸들러가 메인 스레드에서 동작하도록 QTimer 사용
    timer = pg.QtCore.QTimer()
    timer.timeout.connect(lambda: None) 
    timer.start(500)

    # 연결된 장비가 하나도 없으면 에러 메시지 후 종료
    if not any([CONFIG['daq']['available'], CONFIG['radon']['available'], CONFIG['magnetometer']['available']]):
        QMessageBox.critical(None, "오류", "연결된 측정 장비가 없습니다.\n프로그램을 종료합니다.")
        sys.exit()

    main_win = MainWindow(config=CONFIG)
    main_win.show()
    sys.exit(app.exec_())
