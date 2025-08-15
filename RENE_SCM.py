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
import sys
import time
import numpy as np
import os
import math
import signal
import json
import logging
import queue
from collections import deque
from typing import Dict, Any, Optional, List

# --- GUI 및 그래프 관련 ---
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QGridLayout, 
                             QMessageBox, QLabel, QVBoxLayout, QFrame, QStatusBar)
from PyQt5.QtCore import QThread, QObject, pyqtSignal, pyqtSlot, Qt, QTimer
from PyQt5.QtGui import QFont
import pyqtgraph as pg

# ====================================================================================
# 설정 로드 및 초기화
# ====================================================================================

def load_config(config_file="config_scm.json"):
    if not os.path.exists(config_file):
        print(f"Error: Configuration file not found: {config_file}")
        # GUI 환경에서는 메시지 박스도 표시
        # QMessageBox.critical(None, "Configuration Error", f"Configuration file not found: {config_file}")
        sys.exit(1)
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {config_file}: {e}")
        sys.exit(1)

CONFIG = load_config()

# 로깅 설정 (파일 및 콘솔 동시 출력)
logging.basicConfig(
    level=getattr(logging, CONFIG.get('logging_level', 'INFO')),
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.FileHandler("scm_monitoring.log"),
        logging.StreamHandler()
    ]
)

# --- 하드웨어 및 DB 관련 (선택적 임포트) ---
try:
    import nidaqmx
    from nidaqmx.constants import (RTDType, ResistanceConfiguration, TerminalConfiguration, 
                                   ExcitationSource, AcquisitionType)
except ImportError:
    nidaqmx = None
    logging.warning("nidaqmx not found. NI-DAQ functionality disabled.")

try:
    import serial
except ImportError:
    serial = None
    logging.warning("pyserial not found. Serial communication disabled.")

try:
    import pyvisa
except ImportError:
    pyvisa = None
    logging.warning("pyvisa not found. VISA communication disabled.")

try:
    import mariadb
except ImportError:
    mariadb = None
    logging.warning("mariadb not found. Database functionality disabled.")

# ====================================================================================
# [아키텍처 개선] 데이터베이스 처리 워커 (DatabaseWorker)
# ====================================================================================

class DatabaseWorker(QObject):
    """데이터베이스 작업을 전담하는 워커. 생산자-소비자 패턴의 소비자 역할."""
    status_update = pyqtSignal(str)
    error_occurred = pyqtSignal(str)

    # SQL 쿼리 정의
    SQL_INSERT = {
        'DAQ': "INSERT IGNORE INTO SENSOR_DATA (datetime, RTD_1, RTD_2, RTD_3, DIST_1, DIST_2) VALUES (?, ?, ?, ?, ?, ?)",
        'RADON': "INSERT IGNORE INTO RADON_DATA (datetime, mu, sigma) VALUES (?, ?, ?)",
        'MAG': "INSERT IGNORE INTO MAGNETOMETER_DATA (datetime, Bx, By, Bz, B_mag) VALUES (?, ?, ?, ?, ?)"
    }

    def __init__(self, db_config, data_queue: queue.Queue):
        super().__init__()
        self.db_config = db_config
        self.data_queue = data_queue
        self._is_running = True
        self.db_pool = None

    def initialize_db(self):
        if not mariadb or not self.db_config.get('enabled'):
            logging.info("Database connection is disabled or mariadb library not found.")
            return False
        
        # 실제 운영 시 아래 주석을 해제하고 사용합니다.
        try:
            self.status_update.emit("Connecting to MariaDB...")
            # self.db_pool = mariadb.ConnectionPool(
            #     user=self.db_config['user'], password=self.db_config['password'], 
            #     host=self.db_config['host'], port=self.db_config['port'], 
            #     database=self.db_config['database'], pool_name="scm_pool", 
            #     pool_size=self.db_config.get('pool_size', 5)
            # )
            # self._create_tables()
            logging.info("DatabaseWorker initialized (Placeholder Mode).")
            self.status_update.emit("MariaDB Connected (Placeholder).")
            return True
        except Exception as e: # mariadb.Error
            logging.error(f"Failed to initialize MariaDB Pool: {e}")
            self.error_occurred.emit(f"MariaDB 연결 실패: {e}")
            return False

    def _create_tables(self):
        # 테이블 생성 쿼리 정의 (기존 코드 기반)
        queries = [
            "CREATE TABLE IF NOT EXISTS SENSOR_DATA (datetime DATETIME PRIMARY KEY, RTD_1 DECIMAL(6,2), RTD_2 DECIMAL(6,2), RTD_3 DECIMAL(6,2), DIST_1 DECIMAL(7,1), DIST_2 DECIMAL(7,1))",
            "CREATE TABLE IF NOT EXISTS RADON_DATA (datetime DATETIME PRIMARY KEY, mu DECIMAL(6,2), sigma DECIMAL(6,2))",
            "CREATE TABLE IF NOT EXISTS MAGNETOMETER_DATA (datetime DATETIME PRIMARY KEY, Bx DECIMAL(8,2), By DECIMAL(8,2), Bz DECIMAL(8,2), B_mag DECIMAL(8,2))"
        ]
        conn = None
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cursor:
                for query in queries:
                    cursor.execute(query)
            conn.commit()
        except mariadb.Error as e:
            logging.error(f"Error creating tables: {e}")
            raise
        finally:
            if conn: conn.close()

    def run(self):
        if not self.initialize_db():
            return

        while self._is_running:
            try:
                batch = []
                # [최적화] 배치 처리: 최대 50개 항목 수집 또는 1초 대기
                try:
                    for _ in range(50):
                        # 큐에 항목이 없으면 1초 대기, 있으면 즉시 가져오기
                        timeout = 1 if not batch else 0
                        item = self.data_queue.get(block=True, timeout=timeout)
                        
                        if item is None: # 종료 신호
                             self._is_running = False
                             # 종료 신호를 받아도 현재 배치까지는 처리하고 종료
                        
                        batch.append(item)
                        self.data_queue.task_done()
                except queue.Empty:
                    pass # 1초 타임아웃 발생

                if batch:
                    # 종료 신호(None)를 제외한 실제 데이터만 처리
                    actual_data = [i for i in batch if i is not None]
                    if actual_data:
                        self.process_batch(actual_data)

            except Exception as e:
                logging.error(f"DatabaseWorker runtime error: {e}", exc_info=True)
                time.sleep(5)

        logging.info("DatabaseWorker terminated.")

    def process_batch(self, batch):
        # 데이터를 유형별로 분류
        data_by_type = {}
        for item in batch:
            data_type = item['type']
            if data_type not in data_by_type:
                data_by_type[data_type] = []
            data_by_type[data_type].append(item['data'])

        if not self.db_pool:
            # Placeholder 모드일 경우 로그만 출력
            logging.debug(f"DB Placeholder: Processed batch of {len(batch)} records.")
            return

        conn = None
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cursor:
                for data_type, data_list in data_by_type.items():
                    query = self.SQL_INSERT.get(data_type)
                    if query and data_list:
                        # executemany로 일괄 삽입
                        cursor.executemany(query, data_list)
            conn.commit()
            logging.debug(f"Committed batch of {len(batch)} records to DB.")
        except mariadb.Error as e:
            logging.error(f"Database batch insert error: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def stop(self):
        # 큐에 종료 신호(None)를 넣어 워커가 안전하게 종료되도록 함
        self.data_queue.put(None)

# ====================================================================================
# NI-DAQmx 데이터 수집 워커 클래스 (DaqWorker)
# ====================================================================================
class DaqWorker(QObject):
    avg_data_ready = pyqtSignal(float, list, list) 
    raw_data_ready = pyqtSignal(list, list)
    error_occurred = pyqtSignal(str)
    status_update = pyqtSignal(str)

    def __init__(self, daq_config, data_queue: queue.Queue):
        super().__init__()
        self._is_running = True
        self.config = daq_config
        self.data_queue = data_queue # [개선] DB 큐 주입
        self.sampling_rate = self.config.get('sampling_rate', 1000)
        self.num_rtd = len(self.config['rtd']['channels'])
        self.num_volt = len(self.config['volt']['channels'])
        self._reset_buffers()

    def _reset_buffers(self):
        self.temp_rtd_samples = [[] for _ in range(self.num_rtd)]
        self.temp_dist_samples = [[] for _ in range(self.num_volt)]

    def convert_voltage_to_distance(self, voltage, volt_channel_index):
        # (기존 코드와 동일)
        try:
            mapping_info = self.config['volt']['mapping'][volt_channel_index]
            v_min, v_max = mapping_info['volt_range']
            d_min, d_max = mapping_info['dist_range_mm']
            if v_max == v_min: return d_min
            distance = d_min + ((voltage - v_min) / (v_max - v_min)) * (d_max - d_min)
            return np.clip(distance, min(d_min, d_max), max(d_min, d_max))
        except (IndexError, KeyError):
            logging.warning(f"Invalid voltage mapping for channel index {volt_channel_index}.")
            return 0.0

    def run(self):
        """[안정성 강화] 오류 복구 및 재시도 로직 포함."""
        logging.info("DaqWorker started.")
        retry_delay_sec = 10

        while self._is_running:
            try:
                self.status_update.emit("NI-DAQ Task 생성 중...")
                with nidaqmx.Task() as task:
                    self._configure_task(task)
                    self._reset_buffers()
                    
                    self.status_update.emit("NI-DAQ 측정 시작됨")
                    task.start()
                    
                    # 데이터 수집 루프
                    self._read_loop(task)

            except nidaqmx.errors.DaqError as e:
                # 읽기 중 오류 발생 시, 종료 신호가 아니라면 재시도
                if not self._is_running: break
                
                logging.error(f"NI-DAQ Error: {e}. Retrying in {retry_delay_sec}s.")
                self.error_occurred.emit(f"NI-DAQ 오류: {e}. {retry_delay_sec}초 후 재시도...")
                
                # 재시도 대기 (인터럽트 가능)
                self._interruptible_sleep(retry_delay_sec)
                
            except Exception as e:
                logging.exception(f"Unexpected error in DaqWorker: {e}")
                self.error_occurred.emit(f"NI-DAQ 치명적 오류: {e}")
                break # 예상치 못한 오류는 종료

        logging.info("DaqWorker stopped.")

    def _configure_task(self, task):
        # (기존 코드와 동일)
        # RTD 채널 설정
        rtd_device = self.config['rtd']['device']
        rtd_ch_list = [f"{rtd_device}/{ch}" for ch in self.config['rtd']['channels']]
        task.ai_channels.add_ai_rtd_chan(','.join(rtd_ch_list), rtd_type=RTDType.PT_3851, resistance_config=ResistanceConfiguration.THREE_WIRE, current_excit_source=ExcitationSource.INTERNAL, current_excit_val=0.001)
        
        # 전압 채널 설정
        volt_device = self.config['volt']['device']
        volt_ch_list = [f"{volt_device}/{ch}" for ch in self.config['volt']['channels']]
        task.ai_channels.add_ai_voltage_chan(','.join(volt_ch_list), min_val=0.0, max_val=10.0, terminal_config=TerminalConfiguration.DEFAULT)

        # 샘플링 클락 설정
        task.timing.cfg_samp_clk_timing(rate=self.sampling_rate, sample_mode=AcquisitionType.CONTINUOUS, samps_per_chan=self.sampling_rate)

    def _read_loop(self, task):
        while self._is_running:
            data_chunk = task.read(number_of_samples_per_channel=self.sampling_rate)
            
            # 평균화 및 거리 변환
            rtd_values = [np.mean(chunk) for chunk in data_chunk[0:self.num_rtd]]
            voltage_values = [np.mean(chunk) for chunk in data_chunk[self.num_rtd:]]
            dist_values_mm = [self.convert_voltage_to_distance(v, i) for i, v in enumerate(voltage_values)]

            # 실시간 데이터 GUI 전송
            self.raw_data_ready.emit(rtd_values, dist_values_mm)
            self._process_averaging(rtd_values, dist_values_mm)

    def _process_averaging(self, rtd_values, dist_values_mm):
        for i in range(self.num_rtd): self.temp_rtd_samples[i].append(rtd_values[i])
        for i in range(self.num_volt): self.temp_dist_samples[i].append(dist_values_mm[i])

        if self.temp_rtd_samples and len(self.temp_rtd_samples[0]) >= 60:
            timestamp = time.time()
            avg_rtd = [np.mean(ch_data) for ch_data in self.temp_rtd_samples]
            avg_dist = [np.mean(ch_data) for ch_data in self.temp_dist_samples]

            # 1분 평균값 GUI 전송
            self.avg_data_ready.emit(timestamp, avg_rtd, avg_dist)
            
            # [아키텍처 개선] DB 큐로 전송
            self._enqueue_db_data(timestamp, avg_rtd, avg_dist)

            self._reset_buffers()

    def _enqueue_db_data(self, timestamp, avg_rtd, avg_dist):
        dt_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
        # 데이터 포맷팅 (DB 스키마 가정: 3 RTD, 2 DIST)
        # 설정된 채널 수가 스키마보다 적을 경우 None으로 패딩
        padded_rtd = (avg_rtd + [None]*3)[:3]
        padded_dist = (avg_dist + [None]*2)[:2]

        data_tuple = (
            dt_str, 
            round(padded_rtd[0], 2) if padded_rtd[0] is not None else None,
            round(padded_rtd[1], 2) if padded_rtd[1] is not None else None,
            round(padded_rtd[2], 2) if padded_rtd[2] is not None else None,
            round(padded_dist[0], 1) if padded_dist[0] is not None else None,
            round(padded_dist[1], 1) if padded_dist[1] is not None else None
        )
        self.data_queue.put({'type': 'DAQ', 'data': data_tuple})

    def _interruptible_sleep(self, duration):
        """종료 신호에 반응할 수 있는 Sleep 구현."""
        start_time = time.time()
        while self._is_running and (time.time() - start_time) < duration:
            time.sleep(0.5)

    def stop(self):
        logging.info("DaqWorker stop requested.")
        self._is_running = False

# ====================================================================================
# 라돈 측정기 워커 (RadonWorker - QTimer 기반)
# ====================================================================================
class RadonWorker(QObject):
    """[응답성 개선] QTimer를 사용하여 비동기적으로 동작 (time.sleep 제거)."""
    data_ready = pyqtSignal(float, float, float)
    status_update = pyqtSignal(str)
    error_occurred = pyqtSignal(str)

    def __init__(self, radon_config, data_queue: queue.Queue):
        super().__init__()
        self.config = radon_config
        self.data_queue = data_queue
        self.ser_radon = None
        
        # 주기적 측정을 위한 QTimer 설정
        self.measurement_timer = QTimer(self)
        self.measurement_timer.timeout.connect(self.measure_radon)
        self.interval_ms = self.config.get('interval_s', 600) * 1000

    @pyqtSlot()
    def start_worker(self):
        """스레드 시작 시 호출 (기존의 run 대신 사용)."""
        logging.info("RadonWorker starting.")
        if not serial:
            self.error_occurred.emit("pyserial 라이브러리가 설치되지 않았습니다.")
            return

        try:
            self.status_update.emit(f"라돈 센서({self.config['port']}) 연결 중...")
            self.ser_radon = serial.Serial(self.config['port'], 19200, parity=serial.PARITY_NONE, timeout=10)
            
            # 장비 초기화
            self.status_update.emit("라돈 센서 초기화 중...")
            self.ser_radon.write(b'RESET\r\n'); self.ser_radon.readline()
            self.ser_radon.write(b'UNIT 1\r\n'); self.ser_radon.readline()
            
            # 안정화 대기 (QTimer.singleShot 사용 - 블로킹 없음)
            wait_time_s = self.config.get('stabilization_s', 600)
            self.status_update.emit(f"라돈 센서 안정화 대기 중 ({wait_time_s}초)...")
            QTimer.singleShot(wait_time_s * 1000, self.start_measurement_cycle)

        except serial.SerialException as e:
            logging.error(f"Radon Serial Port Error: {e}")
            self.error_occurred.emit(f"Radon 시리얼 오류: {e}")

    def start_measurement_cycle(self):
        if self.ser_radon and self.ser_radon.is_open:
            self.status_update.emit("라돈 측정 시작됨")
            self.measurement_timer.start(self.interval_ms)
            self.measure_radon() # 첫 측정 즉시 실행

    def measure_radon(self):
        if not self.ser_radon or not self.ser_radon.is_open:
            return

        try:
            self.ser_radon.write(b'VALUE?\r\n')
            response = self.ser_radon.readline().decode('ascii')
            timestamp = time.time()

            # 데이터 파싱
            mu = float(response.split(':')[1].split(' ')[1])
            sigma = float(response.split(':')[2].split(' ')[1])
            
            # GUI 업데이트
            self.data_ready.emit(timestamp, mu, sigma)
            # DB 큐 전송
            self._enqueue_db_data(timestamp, mu, sigma)

        except (IndexError, ValueError, serial.SerialException, UnicodeDecodeError) as e:
            logging.error(f"Radon measurement error: {e}. Response: {response if 'response' in locals() else 'N/A'}")
            self.error_occurred.emit(f"라돈 측정 오류: {e}")

    def _enqueue_db_data(self, timestamp, mu, sigma):
        dt_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
        data_tuple = (dt_str, round(mu, 2), round(sigma, 2))
        self.data_queue.put({'type': 'RADON', 'data': data_tuple})

    @pyqtSlot()
    def stop_worker(self):
        logging.info("RadonWorker stopping.")
        if hasattr(self, 'measurement_timer'):
            self.measurement_timer.stop()
        if self.ser_radon and self.ser_radon.is_open:
            self.ser_radon.close()

# ====================================================================================
# 자기장 센서 워커 (MagnetometerWorker - QTimer 기반)
# ====================================================================================
class MagnetometerWorker(QObject):
    """[응답성 개선] QTimer 기반 자기장 워커."""
    avg_data_ready = pyqtSignal(float, list)
    raw_data_ready = pyqtSignal(list)
    error_occurred = pyqtSignal(str)
    status_update = pyqtSignal(str)

    def __init__(self, mag_config, data_queue: queue.Queue):
        super().__init__()
        self.config = mag_config
        self.data_queue = data_queue
        self.instrument = None
        self.rm = None
        self.mag_samples = [[] for _ in range(4)]

        # QTimer 설정
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.measure_magnetometer)
        self.interval_s = self.config.get('interval_s', 1.0)
        self.interval_ms = self.interval_s * 1000

    @pyqtSlot()
    def start_worker(self):
        logging.info("MagnetometerWorker starting.")
        if not pyvisa:
            self.error_occurred.emit("pyvisa 라이브러리가 설치되지 않았습니다.")
            return

        try:
            self.status_update.emit("자기장 센서 연결 중...")
            # 라이브러리 경로가 없으면 기본값 사용
            lib_path = self.config.get('library_path', '')
            self.rm = pyvisa.ResourceManager(lib_path)
            self.instrument = self.rm.open_resource(self.config['resource_name'], timeout=5000)
            self.instrument.read_termination = '\n'; self.instrument.write_termination = '\n'
            
            self._initialize_instrument()
            self.timer.start(self.interval_ms)
            self.status_update.emit("자기장 측정 시작됨")

        except pyvisa.errors.VisaIOError as e:
            self.error_occurred.emit(f"자기장 센서 VISA 오류: {e.description}")
            logging.error(f"VISA connection failed: {e}")

    def _initialize_instrument(self):
        # [최적화] SCPI 명령어 통합
        self.status_update.emit("자기장 센서 설정 중 (SCPI)...")
        try:
            self.instrument.write('*RST')
            time.sleep(1.5) # RST 후 대기
            
            # 명령어 통합 전송
            commands = [
                '*CLS', 'SENSE:RANGE:AUTO ON', 'UNIT MGAUSS', 'TRIGger:SOURce TIMer',
                f'TRIGger:TIMer {self.interval_s}', 'INITiate:CONTinuous ON', 'INITiate'
            ]
            self.instrument.write(';'.join(commands))
            
        except Exception as e:
            logging.error(f"Failed to initialize Magnetometer: {e}")
            raise

    def measure_magnetometer(self):
        if not self.instrument: return

        try:
            # X, Y, Z축 데이터를 한 번에 쿼리
            response = self.instrument.query(':FETCh:SCALar:FLUX:X?;:FETCh:SCALar:FLUX:Y?;:FETCh:SCALar:FLUX:Z?')
            if not response: return

            parts = response.strip().split(';')
            if len(parts) == 3:
                bx = float(parts[0].strip().split(' ')[0])
                by = float(parts[1].strip().split(' ')[0])
                bz = float(parts[2].strip().split(' ')[0])
                b_mag = math.sqrt(bx**2 + by**2 + bz**2)
                
                raw_values = [bx, by, bz, b_mag]
                self.raw_data_ready.emit(raw_values)
                self._process_averaging(raw_values)

        except (ValueError, IndexError, pyvisa.errors.VisaIOError) as e:
            logging.error(f"Magnetometer read/parse error: {e}")

    def _process_averaging(self, raw_values):
        for i in range(4): self.mag_samples[i].append(raw_values[i])

        # 1분 (60초 / 인터벌) 샘플이 모이면 평균 계산
        samples_per_minute = int(60 / self.interval_s)
        if len(self.mag_samples[0]) >= samples_per_minute:
            timestamp = time.time()
            avg_mag = [np.mean(ch_data) for ch_data in self.mag_samples]
            
            # GUI 전송 및 DB 큐 전송
            self.avg_data_ready.emit(timestamp, avg_mag)
            self._enqueue_db_data(timestamp, avg_mag)
            
            self.mag_samples = [[] for _ in range(4)]

    def _enqueue_db_data(self, timestamp, avg_mag):
        dt_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
        data_tuple = (
            dt_str, 
            round(avg_mag[0], 2), round(avg_mag[1], 2), 
            round(avg_mag[2], 2), round(avg_mag[3], 2)
        )
        self.data_queue.put({'type': 'MAG', 'data': data_tuple})

    @pyqtSlot()
    def stop_worker(self):
        logging.info("MagnetometerWorker stopping.")
        if hasattr(self, 'timer'):
            self.timer.stop()
        if self.instrument:
            try:
                self.instrument.write('ABORt')
                self.instrument.close()
            except Exception:
                pass
        if self.rm:
            try:
                self.rm.close()
            except Exception:
                pass

# ====================================================================================
# 메인 GUI 윈도우 클래스 (MainWindow)
# ====================================================================================
class MainWindow(QMainWindow):
    def __init__(self, config, hardware_status):
        super().__init__()
        self.config = config
        self.hardware_status = hardware_status
        self.threads = []

        # [아키텍처 개선] DB 큐 생성
        self.db_queue = queue.Queue()

        self._init_data_structures()
        self._init_ui()
        self._setup_threads()

    def _init_data_structures(self):
        """[성능 최적화] 그래프 데이터 저장을 위해 Deque 사용."""
        days = self.config['gui'].get('max_data_points_days', 7)
        self.max_points_1m = days * 24 * 60
        self.max_points_10m = days * 24 * 6
        
        self.num_rtd = len(self.config.get('daq', {}).get('rtd', {}).get('channels', []))
        self.num_dist = len(self.config.get('daq', {}).get('volt', {}).get('channels', []))

        # List 대신 deque 사용 및 maxlen 설정
        # DAQ Data (1분 평균) - 시간 축은 DAQ 기준으로 사용
        self.time_data = deque(maxlen=self.max_points_1m)
        self.rtd_data = [deque(maxlen=self.max_points_1m) for _ in range(self.num_rtd)]
        self.dist_data = [deque(maxlen=self.max_points_1m) for _ in range(self.num_dist)]
        
        # Radon Data (10분 평균)
        self.radon_time_data = deque(maxlen=self.max_points_10m)
        self.radon_mu_data = deque(maxlen=self.max_points_10m)
        
        # Magnetometer Data (1분 평균)
        self.mag_time_data = deque(maxlen=self.max_points_1m)
        self.mag_data = [deque(maxlen=self.max_points_1m) for _ in range(4)]

    def _init_ui(self):
        self.setWindowTitle("통합 센서 실시간 모니터링 (Optimized)")
        self.setGeometry(100, 100, 1600, 950)
        self.setStatusBar(QStatusBar(self))

        central_widget = QWidget(); self.setCentralWidget(central_widget)
        main_layout = QGridLayout(central_widget)
        plot_layout = QGridLayout()
        main_layout.addLayout(plot_layout, 0, 0, 1, 3)
        
        self.plot_curves = {}
        self._setup_plots(plot_layout)
        main_layout.addLayout(self._create_value_panel(), 0, 3, 1, 1)

    def _setup_plots(self, layout):
        row = 0
        if self.hardware_status['daq']:
            layout.addWidget(self._create_plot_widget("온도 (°C) (1분 평균)", "°C", [f"RTD {i+1}" for i in range(self.num_rtd)], ['r', 'g', 'b']), row, 0); row += 1
            layout.addWidget(self._create_plot_widget("초음파 거리 (mm) (1분 평균)", "mm", [f"거리 {i+1}" for i in range(self.num_dist)], ['c', 'm']), row, 0); row += 1
        
        if self.hardware_status['radon']:
            layout.addWidget(self._create_plot_widget("라돈 (Bq/m³) (10분 간격)", "Bq/m³", ["Radon (μ)"], ['#FF8C00']), row, 0); row += 1
        
        if self.hardware_status['mag']:
            layout.addWidget(self._create_plot_widget("자기장 (mG) (1분 평균)", "mG", ["Bx", "By", "Bz", "|B|"], ['#0000FF', '#008000', '#FF0000', '#000000']), row, 0)

    def _create_plot_widget(self, title, y_label, legend_names, colors):
        # (기존 코드와 동일)
        plot_widget = pg.PlotWidget(title=title)
        plot_widget.setBackground('w')
        plot_widget.addLegend()
        plot_widget.showGrid(x=True, y=True)
        plot_widget.setLabel('left', y_label)
        plot_widget.setAxisItems({'bottom': pg.DateAxisItem(orientation='bottom')})
        for i, name in enumerate(legend_names):
            color = colors[i % len(colors)]
            self.plot_curves[f"{title}_{name}"] = plot_widget.plot(pen=pg.mkPen(color, width=2), name=name)
        return plot_widget

    def _create_value_panel(self):
        # (기존 코드 통합)
        panel_layout = QVBoxLayout(); panel_layout.setAlignment(Qt.AlignTop)
        title_font = QFont("Arial", 16, QFont.Bold); value_font = QFont("Arial", 14)
        
        self.raw_value_labels = {}; self.avg_value_labels = {}

        # 실시간 값 표시 영역
        if self.hardware_status['daq'] or self.hardware_status['mag']:
            panel_layout.addWidget(QLabel("실시간 값 (1초 갱신)"), 0, Qt.AlignTop)
            if self.hardware_status['daq']:
                for i in range(self.num_rtd): self._add_label(panel_layout, f"RTD {i+1}", self.raw_value_labels, value_font)
                for i in range(self.num_dist): self._add_label(panel_layout, f"거리 {i+1}", self.raw_value_labels, value_font)
            if self.hardware_status['mag']:
                for name in ["B_x", "B_y", "B_z", "|B|"]: self._add_label(panel_layout, name, self.raw_value_labels, value_font)
            panel_layout.addSpacing(10); panel_layout.addWidget(self._create_separator())

        # 1분 평균값 표시 영역
        if self.hardware_status['daq'] or self.hardware_status['mag']:
            self.avg_title_label = QLabel("최신 측정값 (1분 평균)"); self.avg_title_label.setFont(title_font)
            panel_layout.addWidget(self.avg_title_label)
            if self.hardware_status['daq']:
                for i in range(self.num_rtd): self._add_label(panel_layout, f"RTD {i+1}", self.avg_value_labels, value_font)
                for i in range(self.num_dist): self._add_label(panel_layout, f"거리 {i+1}", self.avg_value_labels, value_font)
            if self.hardware_status['mag']:
                 for name in ["B_x", "B_y", "B_z", "|B|"]: self._add_label(panel_layout, name, self.avg_value_labels, value_font)
            panel_layout.addSpacing(20); panel_layout.addWidget(self._create_separator()); panel_layout.addSpacing(20)

        # 라돈 값 표시 영역
        if self.hardware_status['radon']:
            self.radon_title_label = QLabel("라돈 측정값 (10분 갱신)"); self.radon_title_label.setFont(title_font)
            panel_layout.addWidget(self.radon_title_label)
            self.radon_value_label = QLabel("μ ± σ: -"); self.radon_value_label.setFont(value_font)
            panel_layout.addWidget(self.radon_value_label)
            self.radon_status_label = QLabel("상태: 대기 중..."); self.radon_status_label.setFont(value_font)
            panel_layout.addWidget(self.radon_status_label)
            
        return panel_layout

    def _add_label(self, layout, name, container, font):
        label = QLabel(f"{name}: -"); label.setFont(font); layout.addWidget(label); container[name] = label

    def _create_separator(self):
        separator = QFrame(); separator.setFrameShape(QFrame.HLine); separator.setFrameShadow(QFrame.Sunken); return separator

    def _setup_threads(self):
        """백그라운드 스레드(센서 및 DB) 설정 및 시작."""
        
        # 1. 데이터베이스 워커 시작 (Loop 기반)
        self._start_worker('DBWorker', DatabaseWorker, CONFIG.get('database', {}), self.db_queue, {}, use_run=True)

        # 2. 센서 워커 시작 (DB 큐 전달)
        if self.hardware_status['daq']:
            # DaqWorker는 연속 수집 특성상 Loop 기반(use_run=True)
            self._start_worker('DaqWorker', DaqWorker, CONFIG['daq'], self.db_queue, {
                'avg_data_ready': self.update_averaged_daq_ui,
                'raw_data_ready': self.update_raw_daq_ui
            }, use_run=True)
        
        if self.hardware_status['radon']:
            # RadonWorker는 QTimer 기반(use_run=False)
            self._start_worker('RadonWorker', RadonWorker, CONFIG['radon'], self.db_queue, {
                'data_ready': self.update_radon_ui,
                'status_update': self.update_radon_status
            }, use_run=False)

        if self.hardware_status['mag']:
             # MagnetometerWorker는 QTimer 기반(use_run=False)
             self._start_worker('MagWorker', MagnetometerWorker, CONFIG['magnetometer'], self.db_queue, {
                 'avg_data_ready': self.update_magnetometer_ui,
                 'raw_data_ready': self.update_raw_magnetometer_ui
             }, use_run=False)

    def _start_worker(self, name, WorkerClass, config, queue_arg, signals, use_run=True):
        """워커 스레드를 생성하고 시작하는 헬퍼 함수."""
        thread = QThread(); thread.setObjectName(name + "Thread")
        worker = WorkerClass(config, queue_arg)
        worker.moveToThread(thread)
        
        # 스레드 시작 시 호출할 메서드 연결
        if use_run:
            thread.started.connect(worker.run)
        else:
            # QTimer 기반 워커는 start_worker 슬롯 호출
            if hasattr(worker, 'start_worker'):
                thread.started.connect(worker.start_worker)

        # 공통 시그널 연결
        if hasattr(worker, 'error_occurred'):
            worker.error_occurred.connect(self.show_error_message)
        
        # 상태 업데이트 연결 (Radon은 자체 UI 요소가 있으므로 중복 연결 방지)
        if hasattr(worker, 'status_update') and 'status_update' not in signals:
             worker.status_update.connect(self.show_status_message)

        # 특정 시그널 연결
        for signal_name, slot in signals.items():
            getattr(worker, signal_name).connect(slot)

        self.threads.append((thread, worker))
        thread.start()

    # --- GUI 업데이트 슬롯 ---
    # [개선] DB 호출 로직 제거됨. Deque 사용으로 pop(0) 불필요.

    @pyqtSlot(float, list, list)
    def update_averaged_daq_ui(self, timestamp, avg_rtd, avg_dist):
        self.time_data.append(timestamp)
        dt_obj = time.strftime('%H:%M:%S', time.localtime(timestamp))
        
        # 타이틀 업데이트 (자기장 데이터가 없는 경우 DAQ가 타이틀 갱신)
        if not self.hardware_status['mag'] and hasattr(self, 'avg_title_label'):
             self.avg_title_label.setText(f"최신 측정값 (갱신: {dt_obj})")
        
        # 데이터 저장 및 라벨/그래프 업데이트
        for i in range(self.num_rtd):
            if i < len(avg_rtd):
                self.rtd_data[i].append(avg_rtd[i])
                self.avg_value_labels[f"RTD {i+1}"].setText(f"RTD {i+1}: {avg_rtd[i]:.2f} °C")
                # Deque를 list로 변환하여 그래프에 전달
                self.plot_curves[f"온도 (°C) (1분 평균)_RTD {i+1}"].setData(x=list(self.time_data), y=list(self.rtd_data[i]))

        for i in range(self.num_dist):
            if i < len(avg_dist):
                self.dist_data[i].append(avg_dist[i])
                self.avg_value_labels[f"거리 {i+1}"].setText(f"거리 {i+1}: {avg_dist[i]:.0f} mm")
                self.plot_curves[f"초음파 거리 (mm) (1분 평균)_거리 {i+1}"].setData(x=list(self.time_data), y=list(self.dist_data[i]))

    @pyqtSlot(list, list)
    def update_raw_daq_ui(self, raw_rtd, raw_dist):
        for i in range(self.num_rtd):
            if i < len(raw_rtd):
                self.raw_value_labels[f"RTD {i+1}"].setText(f"RTD {i+1}: {raw_rtd[i]:.2f} °C")
        for i in range(self.num_dist):
            if i < len(raw_dist):
                self.raw_value_labels[f"거리 {i+1}"].setText(f"거리 {i+1}: {raw_dist[i]:.0f} mm")

    @pyqtSlot(float, list)
    def update_magnetometer_ui(self, timestamp, avg_mag):
        self.mag_time_data.append(timestamp)
        
        dt_obj = time.strftime('%H:%M:%S', time.localtime(timestamp))
        if hasattr(self, 'avg_title_label'):
             self.avg_title_label.setText(f"최신 측정값 (갱신: {dt_obj})")

        for i, n in enumerate(["Bx", "By", "Bz", "|B|"]):
            if i < len(avg_mag):
                self.mag_data[i].append(avg_mag[i])
                label_name = f"B_{'xyz'[i] if i<3 else n}"
                self.avg_value_labels[label_name].setText(f"{label_name}: {avg_mag[i]:.2f} mG")
                self.plot_curves[f"자기장 (mG) (1분 평균)_{n}"].setData(x=list(self.mag_time_data), y=list(self.mag_data[i]))

    @pyqtSlot(list)
    def update_raw_magnetometer_ui(self, raw_mag):
         for i, n in enumerate(["B_x", "B_y", "B_z", "|B|"]): 
             if i < len(raw_mag):
                self.raw_value_labels[n].setText(f"{n}: {raw_mag[i]:.2f} mG")

    @pyqtSlot(float, float, float)
    def update_radon_ui(self, timestamp, mu, sigma):
        self.radon_time_data.append(timestamp); self.radon_mu_data.append(mu)
        self.plot_curves["라돈 (Bq/m³) (10분 간격)_Radon (μ)"].setData(x=list(self.radon_time_data), y=list(self.radon_mu_data))
        
        dt_obj = time.strftime('%H:%M:%S', time.localtime(timestamp))
        if hasattr(self, 'radon_title_label'):
            self.radon_title_label.setText(f"라돈 측정값 (갱신: {dt_obj})")
            self.radon_value_label.setText(f"μ ± σ: {mu:.2f} ± {sigma:.2f} (Bq/m³)")

    @pyqtSlot(str)
    def update_radon_status(self, status):
         if hasattr(self, 'radon_status_label'):
            self.radon_status_label.setText(f"상태: {status}")
         self.show_status_message(f"Radon Status: {status}")

    @pyqtSlot(str)
    def show_status_message(self, message):
        self.statusBar().showMessage(message, 5000)

    @pyqtSlot(str)
    def show_error_message(self, message):
        logging.error(f"Error reported to GUI: {message}")
        QMessageBox.critical(self, "오류 발생", f"에러가 발생했습니다:\n\n{message}")

    def closeEvent(self, event):
        logging.info("Application shutting down...")
        
        # 1. 모든 워커에 종료 신호 전송
        for thread, worker in self.threads:
            # 워커 종류에 따라 적절한 종료 함수 호출 (stop 또는 stop_worker)
            if hasattr(worker, 'stop'):
                worker.stop()
            elif hasattr(worker, 'stop_worker'):
                # QTimer 기반 워커는 stop_worker 슬롯 호출
                # (메인 스레드에서 호출하지만, 하드웨어 해제는 안전하게 처리되도록 각 워커에 구현됨)
                worker.stop_worker()
        
        # 2. 모든 스레드가 종료될 때까지 대기
        # DatabaseWorker는 stop() 호출 시 큐에 None을 넣고, 이를 처리한 후 종료됨.
        for thread, _ in self.threads:
            if thread.isRunning():
                thread.quit()
                if not thread.wait(5000): # 최대 5초 대기
                    logging.warning(f"Thread {thread.objectName()} failed to stop gracefully. Terminating.")
                    thread.terminate()
        event.accept()

# ====================================================================================
# 프로그램 실행 진입점 (Entry Point)
# ====================================================================================

def detect_hardware(config) -> Dict[str, bool]:
    """설정 파일을 기반으로 실제 하드웨어 연결 상태를 확인합니다."""
    status = {'daq': False, 'radon': False, 'mag': False}
    logging.info("Starting hardware detection...")

    # NI-DAQ 감지
    if nidaqmx and 'daq' in config:
        try:
            system = nidaqmx.system.System.local()
            if system.devices:
                logging.info(f"NI DAQ devices detected: {[dev.name for dev in system.devices]}")
                # 실제 설정된 디바이스가 목록에 있는지 확인하는 로직 추가 권장
                status['daq'] = True
            else:
                logging.warning("No NI DAQ devices connected.")
        except Exception as e:
            logging.error(f"NI-DAQmx driver or service error: {e}")
    
    # 라돈 센서 감지
    if serial and 'radon' in config:
        port = config['radon'].get('port')
        if port:
            try:
                # 포트 존재 여부 확인 및 접근 테스트
                s = serial.Serial(port)
                s.close()
                logging.info(f"Radon sensor port ({port}) confirmed.")
                status['radon'] = True
            except serial.SerialException:
                 logging.warning(f"Radon sensor port ({port}) inaccessible or not found.")
        else:
            logging.warning("Radon port not configured.")

    # 자기장 센서 감지
    if pyvisa and 'magnetometer' in config:
        try:
            # 라이브러리 경로가 없으면 기본값 사용
            lib_path = config['magnetometer'].get('library_path', '')
            rm = pyvisa.ResourceManager(lib_path)
            resource_name = config['magnetometer'].get('resource_name')
            if resource_name in rm.list_resources():
                logging.info(f"Magnetometer resource {resource_name} found.")
                status['mag'] = True
            else:
                logging.warning(f"Magnetometer resource {resource_name} not found.")
            rm.close()
        except Exception as e:
            logging.error(f"VISA library or service error: {e}")
            
    return status

if __name__ == '__main__':
    logging.info("="*50 + "\nSCM Integrated Monitoring System Starting (Optimized)\n" + "="*50)
    
    # 하드웨어 상태 확인
    hardware_status = detect_hardware(CONFIG)

    app = QApplication(sys.argv)
    
    # Ctrl+C로 안전하게 종료하기 위한 시그널 핸들러
    signal.signal(signal.SIGINT, lambda signum, frame: QApplication.quit())
    
    # 시그널 핸들러가 메인 스레드에서 동작하도록 QTimer 사용
    timer = QTimer(); timer.start(500); timer.timeout.connect(lambda: None)

    if not any(hardware_status.values()):
        logging.critical("No hardware available based on detection. Exiting.")
        QMessageBox.critical(None, "오류", "연결된 측정 장비가 없거나 설정이 잘못되었습니다.\n로그 파일을 확인하십시오.")
        sys.exit(1)

    main_win = MainWindow(config=CONFIG, hardware_status=hardware_status)
    main_win.show()
    sys.exit(app.exec_())
