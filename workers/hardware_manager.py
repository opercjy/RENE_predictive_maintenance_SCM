# workers/hardware_manager.py

import logging
import serial
import nidaqmx
import pyvisa
from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot, QTimer

class HardwareManager(QObject):
    device_connected = pyqtSignal(str)
    device_disconnected = pyqtSignal(str)  # 향후 사용 가능

    def __init__(self, config):
        super().__init__()
        self.config = config
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.scan)
        self.online = set()
        self.pyvisa_rm = None

    @pyqtSlot()
    def start_scan(self):
        logging.info("HardwareManager scan started in a new thread.")
        if self.config.get('magnetometer', {}).get('enabled') and pyvisa:
            try:
                self.pyvisa_rm = pyvisa.ResourceManager(self.config['magnetometer'].get('library_path', ''))
            except Exception as e:
                logging.error(f"PyVISA ResourceManager init failed: {e}")
        self.scan()
        self.timer.start(5000)

    @pyqtSlot()
    def scan(self):
        newly_detected = self._detect_offline_devices()
        for device_name in newly_detected:
            if device_name not in self.online:
                self.online.add(device_name)
                self.device_connected.emit(device_name)

    def _detect_offline_devices(self):
        newly_detected = set()
        device_names = ['daq', 'radon', 'th_o2', 'arduino', 'magnetometer']
        for name in device_names:
            if name not in self.online and self.config.get(name, {}).get('enabled'):
                if name == 'daq' and nidaqmx:
                    try:
                        if nidaqmx.system.System.local().devices: newly_detected.add('daq')
                    except: pass
                elif name == 'magnetometer' and self.pyvisa_rm:
                    try:
                        if self.config[name].get('resource_name') in self.pyvisa_rm.list_resources():
                            newly_detected.add('magnetometer')
                    except: pass
                elif name in ['radon', 'th_o2', 'arduino'] and serial:
                    if self.config[name].get('port') and self._check_serial(self.config[name]['port']):
                        newly_detected.add(name)
        return newly_detected

    def _check_serial(self, port):
        try:
            s = serial.Serial(port)
            s.close()
            return True
        except serial.SerialException:
            return False

    def stop_scan(self):
        self.timer.stop()
        if self.pyvisa_rm:
            try:
                self.pyvisa_rm.close()
            except Exception as e:
                logging.warning(f"Error closing PyVISA RM: {e}")