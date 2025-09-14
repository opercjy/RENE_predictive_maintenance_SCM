# workers/radon_worker.py

import time
import logging
import serial
from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot, QTimer

class RadonWorker(QObject):
    data_ready = pyqtSignal(float, float, float)
    radon_status_update = pyqtSignal(str) # <<< [1] 변경점: 라돈 전용 상태 신호 추가
    error_occurred = pyqtSignal(str)

    def __init__(self, config, data_queue):
        super().__init__()
        self.config = config
        self.data_queue = data_queue
        self.ser = None
        self.stabilization_timer = QTimer(self)
        self.stabilization_timer.timeout.connect(self._update_stabilization_countdown)
        self.measurement_timer = QTimer(self)
        self.measurement_timer.timeout.connect(self.measure)
        self.interval = int(config.get('interval_s', 600) * 1000)

    @pyqtSlot()
    def start_worker(self):
        try:
            self.ser = serial.Serial(self.config['port'], 19200, timeout=10)
            self.countdown_seconds = self.config.get('stabilization_s', 600)
            status_msg = f"Stabilizing ({self.countdown_seconds}s left)..."
            self.radon_status_update.emit(status_msg) # <<< [2] 변경점: 새 신호로 상태 전송
            self.stabilization_timer.start(1000)
        except serial.SerialException as e:
            self.error_occurred.emit(f"Radon Error: {e}")
            self.radon_status_update.emit("Connection Error")

    def _update_stabilization_countdown(self):
        self.countdown_seconds -= 1
        status_msg = f"Stabilizing ({self.countdown_seconds}s left)..."
        self.radon_status_update.emit(status_msg) # <<< [3] 변경점: 새 신호로 상태 전송
        
        if self.countdown_seconds <= 0:
            self.stabilization_timer.stop()
            self.radon_status_update.emit("Measuring...") # <<< [4] 변경점: 새 신호로 상태 전송
            self.measurement_timer.start(self.interval)
            self.measure()
            
    def measure(self):
        if not (self.ser and self.ser.is_open): return
        try:
            self.ser.write(b'VALUE?\r\n')
            res = self.ser.readline().decode('ascii', 'ignore')
            if res:
                ts = time.time()
                mu = float(res.split(':')[1].split(' ')[1])
                sigma = float(res.split(':')[2].split(' ')[1])
                self.data_ready.emit(ts, mu, sigma)
                self._enqueue_db_data(ts, mu, sigma)
        except Exception as e:
            logging.error(f"Radon parsing error: {e}")
            self.radon_status_update.emit("Parsing Error")

    def _enqueue_db_data(self, ts, mu, sigma):
        dt_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
        self.data_queue.put({'type': 'RADON', 'data': (dt_str, round(mu, 2), round(sigma, 2))})

    def stop_worker(self):
        self.stabilization_timer.stop()
        self.measurement_timer.stop()
        if self.ser:
            self.ser.close()