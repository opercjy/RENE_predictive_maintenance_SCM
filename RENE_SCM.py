# ====================================================================================
# RENE 프로젝트 통합 모니터링 시스템 (RENE-PM)
# 최종 수정일: 2025-09-14
# 버전 v5.1 (최종 수정)
#
# 작성자: [최지영], [전남대학교 물리학과;전남대학교 중성미자 정밀연구센터]
#
# --- 개요 ---
# 이 애플리케이션은 다양한 종류의 물리 센서(온도, 초음파 거리, 아두이노, 라돈, 자기장)로부터
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

# --- 하드웨어 및 DB 관련 (선택적 임포트) ---
try:
    import nidaqmx
    from nidaqmx.constants import (RTDType, ResistanceConfiguration, TerminalConfiguration,
                                   ExcitationSource, AcquisitionType)
except ImportError: nidaqmx = None
try:
    import serial
except ImportError: serial = None
try:
    import pyvisa
except ImportError: pyvisa = None
try:
    import mariadb
except ImportError: mariadb = None
try:
    from pymodbus.client import ModbusSerialClient
    from pymodbus.exceptions import ModbusException
except ImportError: ModbusSerialClient, ModbusException = None, None

# ====================================================================================
# 설정 로드 및 초기화
# ====================================================================================
CONFIG = {}
def load_config(config_file="config_scm.json"):
    global CONFIG
    # __file__이 정의되지 않은 환경(예: 일부 IDE)을 위해 예외 처리
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()
    config_path = os.path.join(script_dir, config_file)
    
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found: {config_path}"); sys.exit(1)
    try:
        with open(config_path, 'r', encoding='utf-8') as f: CONFIG = json.load(f)
        return CONFIG
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {config_path}: {e}"); sys.exit(1)

# ====================================================================================
# 워커 클래스들
# ====================================================================================

class DatabaseWorker(QObject):
    status_update = pyqtSignal(str)
    error_occurred = pyqtSignal(str)
    SQL_INSERT = {
        'DAQ': "INSERT IGNORE INTO LS_DATA (`datetime`, `RTD_1`, `RTD_2`, `DIST_1`, `DIST_2`) VALUES (?, ?, ?, ?, ?)",
        'RADON': "INSERT IGNORE INTO RADON_DATA (`datetime`, `mu`, `sigma`) VALUES (?, ?, ?)",
        'MAG': "INSERT IGNORE INTO MAGNETOMETER_DATA (`datetime`, `Bx`, `By`, `Bz`, `B_mag`) VALUES (?, ?, ?, ?, ?)",
        'TH_O2': "INSERT IGNORE INTO TH_O2_DATA (`datetime`, `temperature`, `humidity`, `oxygen`) VALUES (?, ?, ?, ?)",
        'ARDUINO': "INSERT IGNORE INTO ARDUINO_DATA (`datetime`, `analog_1`, `analog_2`, `analog_3`, `analog_4`, `analog_5`, `digital_status`, `message`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    }
    TABLE_SCHEMAS = [
        """CREATE TABLE IF NOT EXISTS LS_DATA (
            `datetime` DATETIME NOT NULL PRIMARY KEY,
            `RTD_1` FLOAT NULL,
            `RTD_2` FLOAT NULL,
            `DIST_1` FLOAT NULL,
            `DIST_2` FLOAT NULL
        );""",
        """CREATE TABLE IF NOT EXISTS RADON_DATA (
            `datetime` DATETIME NOT NULL PRIMARY KEY,
            `mu` FLOAT NULL,
            `sigma` FLOAT NULL
        );""",
        """CREATE TABLE IF NOT EXISTS MAGNETOMETER_DATA (
            `datetime` DATETIME NOT NULL PRIMARY KEY,
            `Bx` FLOAT NULL,
            `By` FLOAT NULL,
            `Bz` FLOAT NULL,
            `B_mag` FLOAT NULL
        );""",
        """CREATE TABLE IF NOT EXISTS TH_O2_DATA (
            `datetime` DATETIME NOT NULL PRIMARY KEY,
            `temperature` FLOAT NULL,
            `humidity` FLOAT NULL,
            `oxygen` FLOAT NULL
        );""",
        """CREATE TABLE IF NOT EXISTS ARDUINO_DATA (
            `datetime` DATETIME NOT NULL PRIMARY KEY,
            `analog_1` FLOAT NULL,
            `analog_2` FLOAT NULL,
            `analog_3` FLOAT NULL,
            `analog_4` FLOAT NULL,
            `analog_5` FLOAT NULL,
            `digital_status` INT NULL,
            `message` VARCHAR(255) NULL
        );"""
    ]

    def __init__(self, db_config, data_queue: queue.Queue):
        super().__init__()
        self.db_config = db_config
        self.data_queue = data_queue
        self._is_running = True
        self.conn = None
        self.batch_timer = QTimer(self)
        self.batch_timer.timeout.connect(self.process_batch)

    def _connect_and_setup(self):
        try:
            if self.conn:
                self.conn.ping()
                return True
        except (mariadb.Error, AttributeError):
            self.conn = None
        
        try:
            params = {
                'user': self.db_config['user'],
                'password': self.db_config['password'],
                'database': self.db_config['database']
            }
            if self.db_config.get('unix_socket'):
                params['unix_socket'] = self.db_config['unix_socket']
            else:
                params['host'] = self.db_config.get('host', '127.0.0.1')
                params['port'] = self.db_config.get('port', 3306)
            
            self.conn = mariadb.connect(**params)
            self.cursor = self.conn.cursor()
            logging.info("Database connection established.")
            
            logging.info("Checking and creating database tables if not exist...")
            for schema in self.TABLE_SCHEMAS:
                try:
                    self.cursor.execute(schema)
                except mariadb.Error as e:
                    logging.error(f"Failed to execute schema: {schema.strip()}")
                    raise e # 에러를 다시 발생시켜 상위 블록에서 잡도록 함
            self.conn.commit()
            logging.info("Database tables are ready.")
            return True
        except mariadb.Error as e:
            logging.error(f"DB connection/setup error: {e}")
            self.error_occurred.emit(f"DB 연결 또는 설정 오류: {e}")
            self.conn = None
            return False

    @pyqtSlot()
    def run(self):
        if not self.db_config.get('enabled') or not mariadb:
            logging.warning("Database functionality is disabled.")
            return
        
        if not self._connect_and_setup():
            logging.info("Initial DB connection failed. Retrying in 10 seconds...")
            QTimer.singleShot(10000, self.run)
            return

        self.batch_timer.start(60 * 1000)
        logging.info("Database worker started with 1-minute batch processing.")

    @pyqtSlot()
    def process_batch(self):
        if not self._is_running:
            return
        
        try:
            if self.conn is None:
                logging.warning("DB connection is not available. Trying to reconnect...")
                if not self._connect_and_setup():
                    logging.error("DB reconnection failed. Skipping this batch.")
                    return
            else:
                self.conn.ping()
        except mariadb.Error as e:
            logging.error(f"DB connection lost. Attempting to reconnect... Error: {e}")
            if not self._connect_and_setup():
                logging.error("DB reconnection failed. Skipping this batch.")
                return

        batch_size = self.data_queue.qsize()
        if batch_size == 0:
            return
            
        logging.info(f"Processing batch of {batch_size} items...")
        batch = {k: [] for k in self.SQL_INSERT.keys()}
        for _ in range(batch_size):
            try:
                item = self.data_queue.get_nowait()
                if item is None: continue
                if item.get('type') in batch:
                    batch[item['type']].append(item['data'])
                self.data_queue.task_done()
            except queue.Empty:
                break
        try:
            for type_key, data_list in batch.items():
                if data_list:
                    self.cursor.executemany(self.SQL_INSERT[type_key], data_list)
            self.conn.commit()
            logging.info(f"Successfully inserted batch of {batch_size} items.")
        except mariadb.Error as e:
            logging.error(f"DB insert error during batch processing: {e}. Rolling back...")
            self.conn.rollback()

    def stop(self):
        self._is_running = False
        self.batch_timer.stop()
        logging.info("Processing any remaining items in the queue before stopping.")
        self.process_batch()
        if self.conn:
            self.conn.close()
            logging.info("DB connection closed.")

class DaqWorker(QObject):
    avg_data_ready = pyqtSignal(float, dict)
    raw_data_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(self, daq_config, data_queue: queue.Queue):
        super().__init__()
        self._is_running = True
        self.config = daq_config
        self.data_queue = data_queue
        self.sampling_rate = self.config.get('sampling_rate', 1000)
        self.active_modules = []
        self.channel_map = {'rtd': [], 'volt': []}
        self.temp_samples = {}

    def _find_modules_by_sn(self):
        try:
            connected_devices = {dev.serial_num: dev.name for dev in nidaqmx.system.System.local().devices}
            for module_config in self.config.get('modules', []):
                sn_str = module_config['serial_number']
                sn_int = int(sn_str, 16)
                if sn_int in connected_devices:
                    dev_name = connected_devices[sn_int]
                    module_info = module_config.copy()
                    module_info['device_name'] = dev_name
                    self.active_modules.append(module_info)
                    full_ch_names = [f"{dev_name}/{ch}" for ch in module_config['channels']]
                    self.channel_map[module_config['task_type']].extend(full_ch_names)
                    for ch in full_ch_names: self.temp_samples[ch] = []
                    logging.info(f"Activated module {module_config['role']} (SN: {sn_str}) as {dev_name}")
            if not self.active_modules: raise RuntimeError("No DAQ modules specified in the config were found.")
            return True
        except Exception as e:
            self.error_occurred.emit(f"DAQ module scan error: {e}")
            return False

    @pyqtSlot()
    def run(self):
        if not self._find_modules_by_sn(): return
        while self._is_running:
            try:
                with nidaqmx.Task() as task:
                    self._configure_task(task)
                    task.start()
                    self._read_loop(task)
            except nidaqmx.errors.DaqError as e:
                if not self._is_running: break
                self.error_occurred.emit(f"NI-DAQ Error: {e}. Retrying in 10 seconds..."); time.sleep(10)
            except Exception as e:
                if not self._is_running: break
                self.error_occurred.emit(f"NI-DAQ Fatal Error: {e}"); break
    
    def _configure_task(self, task):
        if self.channel_map['rtd']: task.ai_channels.add_ai_rtd_chan(','.join(self.channel_map['rtd']), rtd_type=RTDType.PT_3851, resistance_config=ResistanceConfiguration.THREE_WIRE, current_excit_source=ExcitationSource.INTERNAL, current_excit_val=0.001)
        if self.channel_map['volt']: task.ai_channels.add_ai_voltage_chan(','.join(self.channel_map['volt']), min_val=0.0, max_val=10.0, terminal_config=TerminalConfiguration.DEFAULT)
        task.timing.cfg_samp_clk_timing(rate=self.sampling_rate, sample_mode=AcquisitionType.CONTINUOUS, samps_per_chan=self.sampling_rate)
    
    def _read_loop(self, task):
        all_channels = self.channel_map['rtd'] + self.channel_map['volt']
        while self._is_running:
            data = task.read(number_of_samples_per_channel=self.sampling_rate)
            means = [np.mean(ch) for ch in data]
            raw_data_dict = dict(zip(all_channels, means))
            raw_data_for_ui = {'rtd': [], 'volt': []}
            for mod in self.active_modules:
                for ch_name in mod['channels']:
                    full_ch_name = f"{mod['device_name']}/{ch_name}"
                    if full_ch_name in raw_data_dict: raw_data_for_ui[mod['task_type']].append(raw_data_dict[full_ch_name])
            self.raw_data_ready.emit(raw_data_for_ui)
            self._process_averaging(raw_data_dict)

    def _process_averaging(self, raw_dict):
        for ch, val in raw_dict.items(): self.temp_samples[ch].append(val)
        first_ch = next(iter(self.temp_samples), None)
        if first_ch and len(self.temp_samples[first_ch]) >= 60:
            ts = time.time(); avg_data = {ch: np.mean(s) for ch, s in self.temp_samples.items()}
            avg_rtd_volt = {'rtd': [], 'volt': []}
            for mod in self.active_modules:
                for ch in mod['channels']:
                    full_ch_name = f"{mod['device_name']}/{ch}"
                    if full_ch_name in avg_data: avg_rtd_volt[mod['task_type']].append(avg_data[full_ch_name])
            distances, volt_idx = [], 0
            for mod in self.active_modules:
                if mod['task_type'] == 'volt':
                    for i in range(len(mod['channels'])):
                        distances.append(self.convert_voltage_to_distance(avg_rtd_volt['volt'][volt_idx], mod['mapping'][i])); volt_idx += 1
            avg_rtd_volt['dist'] = distances
            self.avg_data_ready.emit(ts, avg_rtd_volt)
            self._enqueue_db_data(ts, avg_rtd_volt['rtd'], distances)
            for ch in self.temp_samples: self.temp_samples[ch].clear()

    def _enqueue_db_data(self, ts, rtd_vals, dist_vals):
        data = (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts)), round(rtd_vals[0], 2) if rtd_vals else None, round(rtd_vals[1], 2) if len(rtd_vals) > 1 else None, round(dist_vals[0], 1) if dist_vals else None, round(dist_vals[1], 1) if len(dist_vals) > 1 else None)
        self.data_queue.put({'type': 'DAQ', 'data': data})

    def convert_voltage_to_distance(self, v, m):
        try:
            v_min, v_max = m['volt_range']; d_min, d_max = m['dist_range_mm']
            return d_min + ((v - v_min) / (v_max - v_min)) * (d_max - d_min)
        except: return 0.0
            
    def stop(self): self._is_running = False

class RadonWorker(QObject):
    data_ready = pyqtSignal(float, float, float); status_update = pyqtSignal(str); error_occurred = pyqtSignal(str)
    def __init__(self, config, data_queue):
        super().__init__(); self.config = config; self.data_queue = data_queue; self.ser = None
        self.stabilization_timer = QTimer(self); self.stabilization_timer.timeout.connect(self._update_stabilization_countdown)
        self.measurement_timer = QTimer(self); self.measurement_timer.timeout.connect(self.measure)
        self.interval = int(config.get('interval_s', 600) * 1000)

    @pyqtSlot()
    def start_worker(self):
        try:
            self.ser = serial.Serial(self.config['port'], 19200, timeout=10)
            self.countdown_seconds = self.config.get('stabilization_s', 600)
            self.status_update.emit(f"Radon Stabilizing ({self.countdown_seconds}s left)...")
            self.stabilization_timer.start(1000)
        except serial.SerialException as e: self.error_occurred.emit(f"Radon Error: {e}")

    def _update_stabilization_countdown(self):
        self.countdown_seconds -= 1
        if self.countdown_seconds <= 0:
            self.stabilization_timer.stop(); self.status_update.emit("Radon measurement started.")
            self.measurement_timer.start(self.interval); self.measure()
            return
        if self.countdown_seconds % 10 == 0: self.status_update.emit(f"Radon Stabilizing ({self.countdown_seconds}s left)...")

    def measure(self):
        if not (self.ser and self.ser.is_open): return
        try:
            self.ser.write(b'VALUE?\r\n'); res = self.ser.readline().decode('ascii', 'ignore')
            if res:
                ts = time.time(); mu = float(res.split(':')[1].split(' ')[1]); sigma = float(res.split(':')[2].split(' ')[1])
                self.data_ready.emit(ts, mu, sigma); self._enqueue_db_data(ts, mu, sigma)
        except Exception as e: logging.error(f"Radon parsing error: {e}")

    def _enqueue_db_data(self, ts, mu, sigma):
        dt_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
        self.data_queue.put({'type': 'RADON', 'data': (dt_str, round(mu, 2), round(sigma, 2))})

    def stop_worker(self):
        self.stabilization_timer.stop()
        self.measurement_timer.stop()
        if self.ser:
            self.ser.close()

class MagnetometerWorker(QObject):
    avg_data_ready = pyqtSignal(float, list); raw_data_ready = pyqtSignal(dict); error_occurred = pyqtSignal(str)
    def __init__(self, config, data_queue):
        super().__init__(); self.config = config; self.data_queue = data_queue
        self.interval = config.get('interval_s', 1.0); self.samples = [[] for _ in range(4)]
        self._is_running = True; self.inst = None

    def _parse_and_convert_tesla_to_mg(self, response_str: str) -> float:
        try:
            numeric_part = response_str.strip().split(' ')[0]; value_in_tesla = float(numeric_part)
            value_in_milligauss = value_in_tesla * 10_000_000; return value_in_milligauss
        except (ValueError, IndexError) as e:
            logging.error(f"[Magnetometer] Failed to parse or convert response '{response_str}': {e}"); return 0.0

    @pyqtSlot()
    def run(self):
        try:
            logging.info("[Magnetometer] Initializing VISA resource manager.")
            rm = pyvisa.ResourceManager(self.config.get('library_path', ''))
            logging.info(f"[Magnetometer] Attempting to connect to device ({self.config['resource_name']})...")
            self.inst = rm.open_resource(self.config['resource_name'], timeout=5000)
            self.inst.read_termination = '\n'; self.inst.write_termination = '\n'
            logging.info("[Magnetometer] Successfully connected to device.")
            self.inst.write('*RST'); time.sleep(1.5)
            logging.info("[Magnetometer] Starting data acquisition loop.")
            while self._is_running:
                try:
                    response_x = self.inst.query(':MEASure:SCALar:FLUX:X?'); response_y = self.inst.query(':MEASure:SCALar:FLUX:Y?'); response_z = self.inst.query(':MEASure:SCALar:FLUX:Z?')
                    bx = self._parse_and_convert_tesla_to_mg(response_x); by = self._parse_and_convert_tesla_to_mg(response_y); bz = self._parse_and_convert_tesla_to_mg(response_z)
                    raw = {'mag': [bx, by, bz, math.sqrt(bx**2 + by**2 + bz**2)]}
                    self.raw_data_ready.emit(raw); self._process_averaging(raw['mag'])
                    time.sleep(self.interval)
                except pyvisa.errors.VisaIOError as e:
                    if not self._is_running:
                        logging.info("[Magnetometer] VISA session error during shutdown (expected)."); break
                    else: raise e
        except Exception as e:
            error_msg = f"Magnetometer Communication Error: {e}"; logging.error(error_msg); self.error_occurred.emit(error_msg)
        finally:
            if self.inst:
                try: logging.info("[Magnetometer] Closing device connection."); self.inst.close()
                except Exception as e: logging.warning(f"[Magnetometer] Error while closing device: {e}")

    def _process_averaging(self, raw):
        for i in range(4): self.samples[i].append(raw[i])
        if len(self.samples[0]) >= int(60 / self.interval):
            ts = time.time(); avg = [np.mean(ch) for ch in self.samples]
            self.avg_data_ready.emit(ts, avg); self._enqueue_db_data(ts, avg); self.samples = [[] for _ in range(4)]

    def _enqueue_db_data(self, ts, avg):
        dt_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
        self.data_queue.put({'type': 'MAG', 'data': (dt_str, round(avg[0],2), round(avg[1],2), round(avg[2],2), round(avg[3],2))})

    def stop(self): self._is_running = False

class ThO2Worker(QObject):
    avg_data_ready = pyqtSignal(float, float, float, float)
    raw_data_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(self, config, data_queue):
        super().__init__()
        self.config = config
        self.data_queue = data_queue
        self.client = None
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.measure)
        self.interval = int(config.get('interval_s', 1.0) * 1000) 
        self._is_running = False
        self.samples = {'temp': [], 'humi': [], 'o2': []}

    @pyqtSlot()
    def start_worker(self):
        try:
            self.client = ModbusSerialClient(port=self.config['port'], baudrate=self.config.get('baudrate', 4800), timeout=2, parity='N', stopbits=1, bytesize=8)
            if not self.client.connect():
                raise ConnectionError("Connection failed")
            self._is_running = True
            self.timer.start(self.interval)
            logging.info(f"TH/O2 worker started with {self.interval}ms interval.")
        except Exception as e:
            self.error_occurred.emit(f"TH/O2 Sensor Error: {e}")

    def measure(self):
        if not self._is_running:
            return
        try:
            res = self.client.read_holding_registers(address=0, count=3, slave=self.config['modbus_id'])
            if res.isError():
                raise ModbusException(f"Response Error: {res}")
            
            h = res.registers[0] / 10.0
            t_raw = res.registers[1]
            t = ((t_raw - 65536) / 10.0) if t_raw > 32767 else (t_raw / 10.0)
            o = res.registers[2] / 10.0
            
            self.raw_data_ready.emit({'th_o2': {'temp': t, 'humi': h, 'o2': o}})
            self._process_averaging(t, h, o)
        except Exception as e:
            self.error_occurred.emit(f"TH/O2 Comm Error: {e}")
            self.timer.stop()
            if self._is_running:
                QTimer.singleShot(5000, self.timer.start)
    
    def _process_averaging(self, temp, humi, o2):
        self.samples['temp'].append(temp)
        self.samples['humi'].append(humi)
        self.samples['o2'].append(o2)
        
        if len(self.samples['temp']) >= (60 / (self.interval / 1000)):
            ts = time.time()
            avg_t = np.mean(self.samples['temp'])
            avg_h = np.mean(self.samples['humi'])
            avg_o = np.mean(self.samples['o2'])
            
            self.avg_data_ready.emit(ts, avg_t, avg_h, avg_o)
            self._enqueue_db_data(ts, avg_t, avg_h, avg_o)
            
            self.samples = {'temp': [], 'humi': [], 'o2': []}

    def _enqueue_db_data(self, ts, t, h, o):
        dt_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
        self.data_queue.put({'type': 'TH_O2', 'data': (dt_str, round(t, 2), round(h, 2), round(o, 2))})

    def stop_worker(self):
        self._is_running = False
        self.timer.stop()
        if self.client:
            self.client.close()

class ArduinoWorker(QObject):
    avg_data_ready = pyqtSignal(float, dict)
    raw_data_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(self, config, data_queue):
        super().__init__()
        self.config = config
        self.data_queue = data_queue
        self.ser = None
        self.db_cols = [f'analog_{i}' for i in range(1, 6)] + ['digital_status', 'message']
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.measure)
        self.interval = int(config.get('interval_s', 1.0) * 1000)
        self._is_running = False
        self.samples = {key: [] for key in self.config.get('data_mapping', {}).keys()}

    @pyqtSlot()
    def start_worker(self):
        try:
            self.ser = serial.Serial(port=self.config['port'], baudrate=self.config.get('baudrate', 9600), timeout=2)
            self._is_running = True
            self.timer.start(self.interval)
            logging.info(f"Arduino worker started with {self.interval}ms interval.")
        except serial.SerialException as e:
            self.error_occurred.emit(f"Arduino Error: {e}")

    def measure(self):
        if not (self.ser and self.ser.is_open): return
        try:
            line = self.ser.readline().decode('utf-8').strip()
            if line:
                data = {}
                for pair in line.split(','):
                    if ':' in pair:
                        key, val_str = pair.split(':', 1)
                        key = key.strip()
                        val_str = val_str.strip()
                        if key in self.samples:
                            data[key] = None if val_str.upper() == 'NONE' else float(val_str)
                
                if data:
                    self.raw_data_ready.emit({'arduino': data})
                    self._process_averaging(data)
        except Exception as e:
            logging.warning(f"Arduino parsing error: {e}. Raw: '{line}'")

    def _process_averaging(self, data):
        for key, val in data.items():
            if val is not None and key in self.samples:
                self.samples[key].append(val)
        
        first_key = next(iter(self.samples), None)
        if first_key and len(self.samples[first_key]) >= (60 / (self.interval / 1000)):
            ts = time.time()
            avg_data = {}
            for key, val_list in self.samples.items():
                if val_list:
                    avg_data[key] = np.mean(val_list)

            self.avg_data_ready.emit(ts, avg_data)
            self._enqueue_db_data(ts, avg_data)
            
            self.samples = {key: [] for key in self.samples.keys()}

    def _enqueue_db_data(self, ts, data):
        dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
        mapping = self.config.get('data_mapping', {})
        db_data = {col: None for col in self.db_cols}
        for key, val in data.items():
            if key in mapping and val is not None:
                db_data[mapping[key]] = round(val, 2)
        
        # ARDUINO_DATA 테이블 컬럼 개수(8개)에 맞게 튜플 생성
        db_tuple = (dt, db_data.get('analog_1'), db_data.get('analog_2'), db_data.get('analog_3'), 
                    db_data.get('analog_4'), db_data.get('analog_5'), 
                    db_data.get('digital_status'), db_data.get('message'))
        self.data_queue.put({'type': 'ARDUINO', 'data': db_tuple})

    def stop_worker(self):
        self._is_running = False
        self.timer.stop()
        if self.ser:
            self.ser.close()

class HardwareManager(QObject):
    device_connected=pyqtSignal(str); device_disconnected=pyqtSignal(str)
    def __init__(self, config):
        super().__init__(); self.config=config; self.timer=QTimer(self); self.timer.timeout.connect(self.scan); self.online=set()
        self.pyvisa_rm = None
        if self.config.get('magnetometer', {}).get('enabled') and pyvisa:
            try: self.pyvisa_rm = pyvisa.ResourceManager(self.config['magnetometer'].get('library_path', ''))
            except Exception as e: logging.error(f"PyVISA ResourceManager init failed: {e}")
    def start_scan(self, interval=5000): self.scan(); self.timer.start(interval)
    @pyqtSlot()
    def scan(self):
        newly_detected = self._detect_offline_devices()
        for device_name in newly_detected:
            if device_name not in self.online: self.online.add(device_name); self.device_connected.emit(device_name)
    def _detect_offline_devices(self):
        newly_detected = set()
        for name in ['daq', 'radon', 'th_o2', 'arduino', 'magnetometer']:
            if name not in self.online and self.config.get(name, {}).get('enabled'):
                if name == 'daq' and nidaqmx:
                    try:
                        if nidaqmx.system.System.local().devices: newly_detected.add('daq')
                    except: pass
                elif name == 'magnetometer' and self.pyvisa_rm:
                    try:
                        if self.config[name].get('resource_name') in self.pyvisa_rm.list_resources(): newly_detected.add('magnetometer')
                    except: pass
                elif name in ['radon', 'th_o2', 'arduino'] and serial:
                    if self.config[name].get('port') and self._check_serial(self.config[name]['port']): newly_detected.add(name)
        return newly_detected
    def _check_serial(self, port):
        try: s=serial.Serial(port); s.close(); return True
        except: return False
    def stop_scan(self):
        self.timer.stop()
        if self.pyvisa_rm:
            try: self.pyvisa_rm.close()
            except: pass

# ====================================================================================
# Main GUI Window Class
# ====================================================================================
class MainWindow(QMainWindow):
    def __init__(self, config):
        super().__init__()
        self.config=config
        self.db_queue=queue.Queue()
        self.threads={}
        self._init_data()
        self._init_ui()
        self.hw_manager=HardwareManager(self.config)
        self.hw_manager.device_connected.connect(self.activate_sensor)
        self.hw_manager.device_disconnected.connect(self.deactivate_sensor)
        self.hw_manager.start_scan()
        if self.config.get('database',{}).get('enabled'):
            self._start_db_worker()

    def _init_data(self):
        days=self.config.get('gui',{}).get('max_data_points_days',31); m1m=days*24*60; m10m=days*24*6; m2s=days*24*60*30
        self.time_data=deque(maxlen=m1m); self.rtd_data=[deque(maxlen=m1m) for _ in range(2)]; self.dist_data=[deque(maxlen=m1m) for _ in range(2)]
        self.radon_time=deque(maxlen=m10m); self.radon_mu=deque(maxlen=m10m)
        self.mag_time=deque(maxlen=m1m); self.mag_data=[deque(maxlen=m1m) for _ in range(4)]
        self.th_o2_time=deque(maxlen=m2s); self.th_o2_temp=deque(maxlen=m2s); self.th_o2_humi=deque(maxlen=m2s); self.th_o2_o2=deque(maxlen=m2s)
        self.arduino_time=deque(maxlen=m2s); self.arduino_deques={'temp0':deque(maxlen=m2s),'humi0':deque(maxlen=m2s),'temp1':deque(maxlen=m2s),'humi1':deque(maxlen=m2s),'dist':deque(maxlen=m2s)}

    def _init_ui(self):
        self.setWindowTitle("RENE-PM Integrated Monitoring System")
        self.setGeometry(50, 50, 1800, 1000)
        self.setStatusBar(QStatusBar(self))
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QGridLayout(central)
        plot_layout = QGridLayout()
        main_layout.addLayout(plot_layout, 0, 0, 1, 2)
        self.plots = {}
        self.curves = {}
        self.labels = {}
        
        value_panel_frame = QFrame()
        value_panel_frame.setFrameShape(QFrame.StyledPanel)
        value_panel_layout = self._create_value_panel()
        value_panel_frame.setLayout(value_panel_layout)
        
        main_layout.addWidget(value_panel_frame, 0, 2, 1, 1)
        main_layout.setColumnStretch(0, 1)
        main_layout.setColumnStretch(1, 1)
        main_layout.setColumnMinimumWidth(2, 300)
        self._create_ui_elements(plot_layout)

    def _create_plot_group(self, group_key, configs):
        group_widget = QWidget()
        group_layout = QVBoxLayout(group_widget)
        group_layout.setContentsMargins(0,0,0,0)
        for key, title, y_lbl, legends, colors in configs:
            plot = pg.PlotWidget(title=title)
            plot.setBackground('w')
            plot.addLegend()
            plot.showGrid(x=True,y=True)
            plot.setLabel('left',y_lbl)
            plot.setAxisItems({'bottom':pg.DateAxisItem(orientation='bottom')})
            for i, name in enumerate(legends):
                self.curves[f"{key}_{name}"]=plot.plot(pen=pg.mkPen(colors[i],width=2),name=name)
            group_layout.addWidget(plot)
        group_widget.setVisible(False)
        self.plots[group_key]=group_widget
        return group_widget

    def _create_ui_elements(self, layout):
        daq_widget = self._create_plot_group('daq',[('daq_ls_temp',"LS Temperature (°C)","°C",["L_LS_Temp","R_LS_Temp"],['r','g']),('daq_ls_level',"LS Level (mm)","mm",["GdLS Level","GCLS Level"],['c','m'])])
        th_o2_widget = self._create_plot_group('th_o2',[('th_o2_temp_humi',"TH/O2 Sensor (Modbus)","Value",["Temp(°C)","Humi(%)"],['#FF6347','#1E90FF']),('th_o2_o2',"O2 Concentration (Modbus)","%",["Oxygen(%)"],['#32CD32'])])
        arduino_widget = self._create_plot_group('arduino',[('arduino_temp_humi',"Arduino Temp/Humi","Value",["T1(°C)","H1(%)","T2(°C)","H2(%)"],['#E57373','#81D4FA','#F06292','#4FC3F7']),('arduino_dist',"Arduino Distance","cm",["Dist(cm)"],['#66BB6A'])])
        radon_widget = self._create_plot_group('radon',[('radon',"Radon (Bq/m³)","Bq/m³",["Radon (μ)"],['#FF8C00'])])
        mag_widget = self._create_plot_group('mag',[('mag',"Magnetometer (mG)","mG",["Bx","By","Bz","|B|"],['#00F','#0F0','#F00','#000'])])
        
        bottom_left_container = QWidget()
        bottom_left_layout = QVBoxLayout(bottom_left_container)
        bottom_left_layout.setContentsMargins(0, 0, 0, 0)
        bottom_left_layout.addWidget(radon_widget, 1)
        bottom_left_layout.addWidget(mag_widget, 1)
        
        layout.addWidget(daq_widget, 0, 0)
        layout.addWidget(th_o2_widget, 0, 1)
        layout.addWidget(bottom_left_container, 1, 0)
        layout.addWidget(arduino_widget, 1, 1)

    def _create_value_panel(self):
        panel = QVBoxLayout()
        panel.setAlignment(Qt.AlignTop)
        font = QFont("Arial",14)
        groups = {
            "LS (NI-cDAQ)":["L_LS_Temp","R_LS_Temp","GdLS_level","GCLS_level"],
            "Magnetometer":["B_x","B_y","B_z","|B|"],
            "TH/O2 Sensor":["TH_O2_Temp","TH_O2_Humi","TH_O2_Oxygen"],
            "Arduino":["Arduino_Temp1","Arduino_Humi1","Arduino_Temp2","Arduino_Humi2","Arduino_Dist"],
            "Radon":["Radon_Value","Radon_Status"]
        }
        for title, labels in groups.items():
            g_lbl=QLabel(title)
            g_lbl.setFont(QFont("Arial",16,QFont.Bold))
            panel.addWidget(g_lbl)
            line = QFrame()
            line.setFrameShape(QFrame.HLine)
            line.setFrameShadow(QFrame.Sunken)
            panel.addWidget(line)
            for name in labels:
                lbl=QLabel(f"{name.replace('_', ' ')}: -")
                lbl.setFont(font)
                self.labels[name]=lbl
                lbl.setVisible(False)
                panel.addWidget(lbl)
            panel.addSpacing(20)
        return panel

    @pyqtSlot(str)
    def activate_sensor(self, name):
        ui_map = {
            'daq': (['daq'], ["L_LS_Temp", "R_LS_Temp", "GdLS_level", "GCLS_level"]),
            'radon': (['radon'], ["Radon_Value", "Radon_Status"]),
            'magnetometer': (['mag'], ["B_x", "B_y", "B_z", "|B|"]),
            'th_o2': (['th_o2'], ["TH_O2_Temp", "TH_O2_Humi", "TH_O2_Oxygen"]),
            'arduino': (['arduino'], ["Arduino_Temp1", "Arduino_Humi1", "Arduino_Temp2", "Arduino_Humi2", "Arduino_Dist"])
        }
        if name in ui_map:
            for key in ui_map[name][0]:
                if key in self.plots: self.plots[key].setVisible(True)
            for key in ui_map[name][1]:
                if key in self.labels: self.labels[key].setVisible(True)
        self._start_worker(name)

    @pyqtSlot(str)
    def deactivate_sensor(self, name):
        ui_map = {
            'daq': (['daq'], ["L_LS_Temp", "R_LS_Temp", "GdLS_level", "GCLS_level"]),
            'radon': (['radon'], ["Radon_Value", "Radon_Status"]),
            'magnetometer': (['mag'], ["B_x", "B_y", "B_z", "|B|"]),
            'th_o2': (['th_o2'], ["TH_O2_Temp", "TH_O2_Humi", "TH_O2_Oxygen"]),
            'arduino': (['arduino'], ["Arduino_Temp1", "Arduino_Humi1", "Arduino_Temp2", "Arduino_Humi2", "Arduino_Dist"])
        }
        if name in ui_map:
            for key in ui_map[name][0]:
                if key in self.plots: self.plots[key].setVisible(False)
            for key in ui_map[name][1]:
                if key in self.labels: self.labels[key].setVisible(False)
        self._stop_worker(name)

    def _start_db_worker(self):
        if 'db' in self.threads: return
        thread=QThread()
        worker=DatabaseWorker(self.config['database'], self.db_queue)
        worker.moveToThread(thread)
        worker.status_update.connect(self.statusBar().showMessage)
        worker.error_occurred.connect(self.show_error)
        thread.started.connect(worker.run)
        thread.start()
        self.threads['db']=(thread,worker)

    def _start_worker(self, name):
        if name in self.threads: return
        w_map = {'daq':(DaqWorker,True),'radon':(RadonWorker,False),'magnetometer':(MagnetometerWorker,True),'th_o2':(ThO2Worker,False),'arduino':(ArduinoWorker,False)}
        if name not in w_map: return
        
        WClass, use_run = w_map[name]
        thread = QThread()
        worker = WClass(self.config.get(name, {}), self.db_queue)
        worker.moveToThread(thread)
        
        if hasattr(worker, 'error_occurred'): worker.error_occurred.connect(self.show_error)
        if hasattr(worker, 'status_update'): worker.status_update.connect(self.statusBar().showMessage)
        
        s_map = {'daq': 'avg_data_ready', 'radon': 'data_ready', 'magnetometer': 'avg_data_ready', 'th_o2': 'avg_data_ready', 'arduino': 'avg_data_ready'}
        slot_map = {'daq': self.update_daq_ui, 'radon': self.update_radon_ui, 'magnetometer': self.update_mag_ui, 'th_o2': self.update_th_o2_ui, 'arduino': self.update_arduino_ui}
        
        if name in s_map and hasattr(worker, s_map[name]):
            getattr(worker, s_map[name]).connect(slot_map[name])
            
        if name in ['daq', 'magnetometer', 'th_o2', 'arduino']:
            if hasattr(worker, 'raw_data_ready'):
                worker.raw_data_ready.connect(self.update_raw_ui)

        thread.started.connect(worker.run if use_run else worker.start_worker)
        thread.start()
        self.threads[name] = (thread, worker)

    def _stop_worker(self, name):
        if name in self.threads:
            thread,worker=self.threads.pop(name)
            stop_method = getattr(worker, 'stop', getattr(worker, 'stop_worker', None))
            if stop_method:
                stop_method()
            thread.quit()
            thread.wait(3000)
    
    def _convert_daq_voltage_to_distance(self, v, mapping_index):
        try:
            volt_module = next((mod for mod in self.config['daq']['modules'] if mod['task_type'] == 'volt'), None)
            if volt_module:
                m = volt_module['mapping'][mapping_index]
                v_min, v_max = m['volt_range']
                d_min, d_max = m['dist_range_mm']
                return d_min + ((v - v_min) / (v_max - v_min)) * (d_max - d_min)
        except (IndexError, KeyError, StopIteration) as e:
            logging.warning(f"Failed to convert voltage to distance: {e}")
        return 0.0

    @pyqtSlot(float,dict)
    def update_daq_ui(self, ts, data):
        rtd, dist_vals = data.get('rtd', []), data.get('dist', [])
        self.time_data.append(ts)
        if len(rtd) > 0: self.rtd_data[0].append(rtd[0]); self.curves["daq_ls_temp_L_LS_Temp"].setData(x=list(self.time_data),y=list(self.rtd_data[0]))
        if len(rtd) > 1: self.rtd_data[1].append(rtd[1]); self.curves["daq_ls_temp_R_LS_Temp"].setData(x=list(self.time_data),y=list(self.rtd_data[1]))
        if len(dist_vals) > 0: self.dist_data[0].append(dist_vals[0]); self.curves["daq_ls_level_GdLS Level"].setData(x=list(self.time_data),y=list(self.dist_data[0]))
        if len(dist_vals) > 1: self.dist_data[1].append(dist_vals[1]); self.curves["daq_ls_level_GCLS Level"].setData(x=list(self.time_data),y=list(self.dist_data[1]))

    @pyqtSlot(float,float,float)
    def update_radon_ui(self, ts, mu, sigma):
        self.radon_time.append(ts)
        self.radon_mu.append(mu)
        self.curves["radon_Radon (μ)"].setData(x=list(self.radon_time),y=list(self.radon_mu))
        self.labels["Radon_Value"].setText(f"μ ± σ: {mu:.2f} ± {sigma:.2f}")

    @pyqtSlot(float,list)
    def update_mag_ui(self, ts, mag):
        self.mag_time.append(ts)
        for i in range(4):
            self.mag_data[i].append(mag[i])
            self.curves[f"mag_{['Bx','By','Bz','|B|'][i]}"].setData(x=list(self.mag_time),y=list(self.mag_data[i]))

    @pyqtSlot(float,float,float,float)
    def update_th_o2_ui(self, ts, temp, humi, o2):
        self.th_o2_time.append(ts); self.th_o2_temp.append(temp); self.th_o2_humi.append(humi); self.th_o2_o2.append(o2)
        self.curves["th_o2_temp_humi_Temp(°C)"].setData(x=list(self.th_o2_time),y=list(self.th_o2_temp))
        self.curves["th_o2_temp_humi_Humi(%)"].setData(x=list(self.th_o2_time),y=list(self.th_o2_humi))
        self.curves["th_o2_o2_Oxygen(%)"].setData(x=list(self.th_o2_time),y=list(self.th_o2_o2))

    @pyqtSlot(float,dict)
    def update_arduino_ui(self, ts, data):
        self.arduino_time.append(ts)
        key_map = {'temp0': 'temp0', 'humi0': 'humi0', 'temp1': 'temp1', 'humi1': 'humi1', 'dist': 'dist'}
        for key, deq_key in key_map.items():
            if data.get(key) is not None:
                self.arduino_deques[deq_key].append(data[key])

        plot_map = {
            "arduino_temp_humi_T1(°C)": 'temp0', "arduino_temp_humi_H1(%)": 'humi0',
            "arduino_temp_humi_T2(°C)": 'temp1', "arduino_temp_humi_H2(%)": 'humi1',
            "arduino_dist_Dist(cm)": 'dist'
        }
        for curve_key, deq_key in plot_map.items():
            deq = self.arduino_deques[deq_key]
            if deq:
                self.curves[curve_key].setData(x=list(self.arduino_time)[-len(deq):], y=list(deq))

    @pyqtSlot(dict)
    def update_raw_ui(self, data):
        if 'rtd' in data or 'volt' in data:
            rtd, volt = data.get('rtd', []), data.get('volt', [])
            if len(rtd) > 0: self.labels["L_LS_Temp"].setText(f"L LS Temp: {rtd[0]:.2f} °C")
            if len(rtd) > 1: self.labels["R_LS_Temp"].setText(f"R LS Temp: {rtd[1]:.2f} °C")
            if len(volt) > 0: self.labels["GdLS_level"].setText(f"GdLS level: {self._convert_daq_voltage_to_distance(volt[0], 0):.1f} mm")
            if len(volt) > 1: self.labels["GCLS_level"].setText(f"GCLS level: {self._convert_daq_voltage_to_distance(volt[1], 1):.1f} mm")
        
        if 'mag' in data:
            mag = data.get('mag', [])
            keys = ["B_x", "B_y", "B_z", "|B|"]
            labels = ["X", "Y", "Z", "|B|"]
            for i, key in enumerate(keys):
                if len(mag) > i:
                    self.labels[key].setText(f"{labels[i]}: {mag[i]:.2f} mG")

        if 'th_o2' in data:
            th_o2_data = data['th_o2']
            if 'temp' in th_o2_data: self.labels["TH_O2_Temp"].setText(f"Temp: {th_o2_data['temp']:.2f} °C")
            if 'humi' in th_o2_data: self.labels["TH_O2_Humi"].setText(f"Humi: {th_o2_data['humi']:.2f} %")
            if 'o2' in th_o2_data: self.labels["TH_O2_Oxygen"].setText(f"Oxygen: {th_o2_data['o2']:.2f} %")
            
        if 'arduino' in data:
            arduino_data = data['arduino']
            if 'temp0' in arduino_data and arduino_data['temp0'] is not None: self.labels["Arduino_Temp1"].setText(f"Temp1: {arduino_data['temp0']:.2f} °C")
            if 'humi0' in arduino_data and arduino_data['humi0'] is not None: self.labels["Arduino_Humi1"].setText(f"Humi1: {arduino_data['humi0']:.2f} %")
            if 'temp1' in arduino_data and arduino_data['temp1'] is not None: self.labels["Arduino_Temp2"].setText(f"Temp2: {arduino_data['temp1']:.2f} °C")
            if 'humi1' in arduino_data and arduino_data['humi1'] is not None: self.labels["Arduino_Humi2"].setText(f"Humi2: {arduino_data['humi1']:.2f} %")
            if 'dist' in arduino_data and arduino_data['dist'] is not None: self.labels["Arduino_Dist"].setText(f"Dist: {arduino_data['dist']:.1f} cm")

    def show_error(self, msg):
        logging.error(f"GUI Error: {msg}")
        # GUI 스레드에서 메시지 박스를 표시하도록 보장
        if QThread.currentThread() is self.thread():
            QMessageBox.critical(self, "Error", msg)
        else:
            # 다른 스레드에서 호출된 경우, 메인 스레드에서 실행하도록 스케줄링
            QTimer.singleShot(0, lambda: QMessageBox.critical(self, "Error", msg))

    def closeEvent(self, event):
        logging.info("Application closing... Stopping all worker threads.")
        if 'db' in self.threads: self.db_queue.put(None) 
        
        active_threads = list(self.threads.keys())
        for name in active_threads:
            self._stop_worker(name)

        logging.info("All threads stopped. Exiting.")
        event.accept()

# ====================================================================================
# Main Entry Point
# ====================================================================================
if __name__ == '__main__':
    load_config()
    log_level = CONFIG.get('logging_level', 'INFO').upper()
    log_filename = "rene_pm.log"
    # 로그 파일 초기화 (새 실행마다 새로 작성)
    with open(log_filename, 'w'):
        pass
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), 
                        format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s', 
                        handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])
    
    logging.info("="*50 + "\nRENE-PM Integrated Monitoring System Starting\n" + "="*50)
    
    app = QApplication(sys.argv)
    
    # SIGINT (Ctrl+C) 시그널 핸들러 설정
    signal.signal(signal.SIGINT, lambda s, f: QApplication.quit())
    # QTimer를 사용하여 Python 시그널 핸들러가 GUI 이벤트 루프와 함께 작동하도록 보장
    timer = QTimer()
    timer.start(500)
    timer.timeout.connect(lambda: None)

    main_win = MainWindow(config=CONFIG)
    main_win.show()
    sys.exit(app.exec_())
