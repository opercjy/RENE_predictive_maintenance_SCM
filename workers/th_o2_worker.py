# workers/th_o2_worker.py

import time
import numpy as np
import logging
from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusException
from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot, QTimer

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
        
        if len(self.samples['temp']) >= (30 / (self.interval / 1000)):
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