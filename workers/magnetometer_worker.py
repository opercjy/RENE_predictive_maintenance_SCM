# workers/magnetometer_worker.py

import time
import math
import numpy as np
import logging
import pyvisa
from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot

class MagnetometerWorker(QObject):
    avg_data_ready = pyqtSignal(float, list)
    raw_data_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)

    def __init__(self, config, data_queue):
        super().__init__()
        self.config = config
        self.data_queue = data_queue
        self.interval = config.get('interval_s', 1.0)
        self.samples = [[] for _ in range(4)]
        self._is_running = True
        self.inst = None

    def _parse_and_convert_tesla_to_mg(self, response_str: str) -> float:
        try:
            numeric_part = response_str.strip().split(' ')[0]
            value_in_tesla = float(numeric_part)
            value_in_milligauss = value_in_tesla * 10_000_000
            return value_in_milligauss
        except (ValueError, IndexError) as e:
            logging.error(f"[Magnetometer] Failed to parse or convert response '{response_str}': {e}")
            return 0.0

    @pyqtSlot()
    def run(self):
        try:
            rm = pyvisa.ResourceManager(self.config.get('library_path', ''))
            self.inst = rm.open_resource(self.config['resource_name'], timeout=5000)
            self.inst.read_termination = '\n'
            self.inst.write_termination = '\n'
            logging.info("[Magnetometer] Successfully connected to device.")
            self.inst.write('*RST'); time.sleep(1.5)
            while self._is_running:
                try:
                    response_x = self.inst.query(':MEASure:SCALar:FLUX:X?')
                    response_y = self.inst.query(':MEASure:SCALar:FLUX:Y?')
                    response_z = self.inst.query(':MEASure:SCALar:FLUX:Z?')
                    bx = self._parse_and_convert_tesla_to_mg(response_x)
                    by = self._parse_and_convert_tesla_to_mg(response_y)
                    bz = self._parse_and_convert_tesla_to_mg(response_z)
                    b_mag = math.sqrt(bx**2 + by**2 + bz**2)
                    raw = {'mag': [bx, by, bz, b_mag]}
                    self.raw_data_ready.emit(raw)
                    self._process_averaging(raw['mag'])
                    time.sleep(self.interval)
                except pyvisa.errors.VisaIOError as e:
                    if not self._is_running: break
                    else: raise e
        except Exception as e:
            self.error_occurred.emit(f"Magnetometer Communication Error: {e}")
        finally:
            if self.inst:
                self.inst.close()

    def _process_averaging(self, raw):
        for i in range(4):
            self.samples[i].append(raw[i])
        if len(self.samples[0]) >= int(60 / self.interval):
            ts = time.time()
            avg = [np.mean(ch) for ch in self.samples]
            self.avg_data_ready.emit(ts, avg)
            self._enqueue_db_data(ts, avg)
            self.samples = [[] for _ in range(4)]

    def _enqueue_db_data(self, ts, avg):
        dt_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
        self.data_queue.put({'type': 'MAG', 'data': (dt_str, round(avg[0],2), round(avg[1],2), round(avg[2],2), round(avg[3],2))})

    def stop(self):
        self._is_running = False