# workers/arduino_worker.py

import time
import numpy as np
import logging
import serial
from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot, QTimer

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
        if first_key and len(self.samples[first_key]) >= (30 / (self.interval / 1000)):
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
        
        db_tuple = (dt, db_data.get('analog_1'), db_data.get('analog_2'), db_data.get('analog_3'), 
                    db_data.get('analog_4'), db_data.get('analog_5'), 
                    db_data.get('digital_status'), db_data.get('message'))
        self.data_queue.put({'type': 'ARDUINO', 'data': db_tuple})

    def stop_worker(self):
        self._is_running = False
        self.timer.stop()
        if self.ser:
            self.ser.close()