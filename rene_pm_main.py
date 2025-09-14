# main.py

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

# --- 하드웨어 관련 (선택적 임포트) ---
try:
    import nidaqmx
except ImportError: nidaqmx = None
try:
    import serial
except ImportError: serial = None
try:
    import pyvisa
except ImportError: pyvisa = None

# --- 워커 클래스 임포트 ---

from workers import (DatabaseWorker, DaqWorker, RadonWorker, MagnetometerWorker, 
                     ThO2Worker, ArduinoWorker)
from workers.hardware_manager import HardwareManager

# ====================================================================================
# 설정 로드 및 초기화
# ====================================================================================
CONFIG = {}
def load_config(config_file="config_scm.json"):
    global CONFIG
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
# Main GUI Window Class
# ====================================================================================

class MainWindow(QMainWindow):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.db_queue = queue.Queue()
        self.threads = {}

        self.latest_raw_values = {}
        self.plot_dirty_flags = {}
        self.ui_update_timer = QTimer(self)
        self.ui_update_timer.timeout.connect(self._update_gui)
        self.ui_update_timer.start(500)

        self.clock_timer = QTimer(self)
        self.clock_timer.timeout.connect(self._update_clock)
        self.clock_timer.start(1000)

        self._init_data()
        self._init_ui()
        self._init_curve_data_map()

        self.hw_thread = QThread()
        self.hw_manager = HardwareManager(self.config)
        self.hw_manager.moveToThread(self.hw_thread)
        self.hw_manager.device_connected.connect(self.activate_sensor)
        self.hw_thread.started.connect(self.hw_manager.start_scan)
        self.hw_thread.start()
        
        if self.config.get('database',{}).get('enabled'):
            self._start_db_worker()

    def _init_data(self):
        days = self.config.get('gui', {}).get('max_data_points_days', 31)
        self.m1m_len = days * 24 * 60
        self.m10m_len = days * 24 * 6
        
        self.rtd_data = np.full((self.m1m_len, 3), np.nan)
        self.dist_data = np.full((self.m1m_len, 3), np.nan)
        self.radon_data = np.full((self.m10m_len, 2), np.nan)
        self.mag_data = np.full((self.m1m_len, 5), np.nan)
        self.th_o2_data = np.full((self.m1m_len, 4), np.nan)
        self.arduino_data = np.full((self.m1m_len, 6), np.nan)
        
        self.pointers = {'daq': 0, 'radon': 0, 'mag': 0, 'th_o2': 0, 'arduino': 0}
        self.max_lens = {'daq': self.m1m_len, 'radon': self.m10m_len, 'mag': self.m1m_len, 'th_o2': self.m1m_len, 'arduino': self.m1m_len}

    def _init_ui(self):
        self.setWindowTitle("RENE-PM")
        self.setGeometry(50, 50, 1800, 1000)
        
        self.status_bar = QStatusBar(self)
        self.setStatusBar(self.status_bar)
        
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        title_label = QLabel("RENE-PM Integrated Monitoring System")
        title_label.setFont(QFont("Arial", 20, QFont.Bold))
        title_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(title_label)

        grid_layout_container = QWidget()
        main_layout.addWidget(grid_layout_container)
        
        main_grid = QGridLayout(grid_layout_container)
        plot_layout = QGridLayout()
        main_grid.addLayout(plot_layout, 0, 0, 1, 2)
        
        self.plots, self.curves, self.labels = {}, {}, {}
        
        value_panel_frame = QFrame()
        value_panel_frame.setFrameShape(QFrame.StyledPanel)
        value_panel_layout = self._create_value_panel()
        value_panel_frame.setLayout(value_panel_layout)
        
        main_grid.addWidget(value_panel_frame, 0, 2, 1, 1)
        main_grid.setColumnStretch(0, 1)
        main_grid.setColumnStretch(1, 1)
        main_grid.setColumnMinimumWidth(2, 320)
        self._create_ui_elements(plot_layout)

        shifter_text = self.config.get("gui", {}).get("shifter_name", "Unknown Shifter")
        self.shifter_label = QLabel(f" Shifter: {shifter_text} ")
        self.clock_label = QLabel()
        self.status_bar.addPermanentWidget(self.shifter_label)
        self.status_bar.addPermanentWidget(self.clock_label)
        self._update_clock()

    def _init_curve_data_map(self):
        self.curve_data_map = {
            "daq_ls_temp_L_LS_Temp": (self.rtd_data[:, 0], self.rtd_data[:, 1]),
            "daq_ls_temp_R_LS_Temp": (self.rtd_data[:, 0], self.rtd_data[:, 2]),
            "daq_ls_level_GdLS Level": (self.dist_data[:, 0], self.dist_data[:, 1]),
            "daq_ls_level_GCLS Level": (self.dist_data[:, 0], self.dist_data[:, 2]),
            "radon_Radon (μ)": (self.radon_data[:, 0], self.radon_data[:, 1]),
            "mag_Bx": (self.mag_data[:, 0], self.mag_data[:, 1]),
            "mag_By": (self.mag_data[:, 0], self.mag_data[:, 2]),
            "mag_Bz": (self.mag_data[:, 0], self.mag_data[:, 3]),
            "mag_|B|": (self.mag_data[:, 0], self.mag_data[:, 4]),
            "th_o2_temp_humi_Temp(°C)": (self.th_o2_data[:, 0], self.th_o2_data[:, 1]),
            "th_o2_temp_humi_Humi(%)": (self.th_o2_data[:, 0], self.th_o2_data[:, 2]),
            "th_o2_o2_Oxygen(%)": (self.th_o2_data[:, 0], self.th_o2_data[:, 3]),
            "arduino_temp_humi_T1(°C)": (self.arduino_data[:, 0], self.arduino_data[:, 1]),
            "arduino_temp_humi_H1(%)": (self.arduino_data[:, 0], self.arduino_data[:, 2]),
            "arduino_temp_humi_T2(°C)": (self.arduino_data[:, 0], self.arduino_data[:, 3]),
            "arduino_temp_humi_H2(%)": (self.arduino_data[:, 0], self.arduino_data[:, 4]),
            "arduino_dist_Dist(cm)": (self.arduino_data[:, 0], self.arduino_data[:, 5]),
        }

    def _create_plot_group(self, group_key, configs):
        color_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
        group_widget = QWidget()
        group_layout = QVBoxLayout(group_widget)
        group_layout.setContentsMargins(0,0,0,0)
        color_index = 0
        for key, title, y_lbl, legends, _ in configs:
            plot = pg.PlotWidget()
            plot.setBackground('w')
            plot.setTitle(title, size='12pt')
            plot.getAxis('left').setLabel(y_lbl, **{'font-size': '10pt'})
            plot.getAxis('bottom').setLabel('Time', **{'font-size': '10pt'})
            plot.getAxis('left').setTickFont(QFont("Arial", 9))
            plot.getAxis('bottom').setTickFont(QFont("Arial", 9))
            plot.showGrid(x=True,y=True,alpha=0.3)
            legend = plot.addLegend(offset=(10,10))
            legend.setBrush(pg.mkBrush(255, 255, 255, 150))
            for i, name in enumerate(legends):
                pen_color = color_palette[color_index % len(color_palette)]
                self.curves[f"{key}_{name}"]=plot.plot(pen=pg.mkPen(pen_color, width=2.5), name=name)
                color_index += 1
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
        groups = {"LS (NI-cDAQ)":["L_LS_Temp","R_LS_Temp","GdLS_level","GCLS_level"],"Magnetometer":["B_x","B_y","B_z","|B|"],"TH/O2 Sensor":["TH_O2_Temp","TH_O2_Humi","TH_O2_Oxygen"],"Arduino":["Arduino_Temp1","Arduino_Humi1","Arduino_Temp2","Arduino_Humi2","Arduino_Dist"],"Radon":["Radon_Value","Radon_Status"]}
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

    @pyqtSlot()
    def _update_clock(self):
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        self.clock_label.setText(f" {now} ")

    @pyqtSlot(str)
    def _update_radon_status(self, status_text):
        if "Radon_Status" in self.labels:
            self.labels["Radon_Status"].setText(f"Status: {status_text}")

    @pyqtSlot(str)
    def activate_sensor(self, name):
        ui_map = {'daq': (['daq'], ["L_LS_Temp", "R_LS_Temp", "GdLS_level", "GCLS_level"]),'radon': (['radon'], ["Radon_Value", "Radon_Status"]),'magnetometer': (['mag'], ["B_x", "B_y", "B_z", "|B|"]),'th_o2': (['th_o2'], ["TH_O2_Temp", "TH_O2_Humi", "TH_O2_Oxygen"]),'arduino': (['arduino'], ["Arduino_Temp1", "Arduino_Humi1", "Arduino_Temp2", "Arduino_Humi2", "Arduino_Dist"])}
        if name in ui_map:
            for key in ui_map[name][0]:
                if key in self.plots: self.plots[key].setVisible(True)
            for key in ui_map[name][1]:
                if key in self.labels: self.labels[key].setVisible(True)
        self._start_worker(name)

    @pyqtSlot(str)
    def deactivate_sensor(self, name):
        ui_map = {'daq': (['daq'], ["L_LS_Temp", "R_LS_Temp", "GdLS_level", "GCLS_level"]),'radon': (['radon'], ["Radon_Value", "Radon_Status"]),'magnetometer': (['mag'], ["B_x", "B_y", "B_z", "|B|"]),'th_o2': (['th_o2'], ["TH_O2_Temp", "TH_O2_Humi", "TH_O2_Oxygen"]),'arduino': (['arduino'], ["Arduino_Temp1", "Arduino_Humi1", "Arduino_Temp2", "Arduino_Humi2", "Arduino_Dist"])}
        if name in ui_map:
            for key in ui_map[name][0]:
                if key in self.plots: self.plots[key].setVisible(False)
            for key in ui_map[name][1]:
                if key in self.labels: self.labels[key].setVisible(False)
        self._stop_worker(name)

    def _start_db_worker(self):
        if 'db' in self.threads: return
        thread=QThread(); worker=DatabaseWorker(self.config['database'], self.db_queue)
        worker.moveToThread(thread); worker.status_update.connect(self.status_bar.showMessage)
        worker.error_occurred.connect(self.show_error); thread.started.connect(worker.run); thread.start()
        self.threads['db']=(thread,worker)

    def _start_worker(self, name):
        if name in self.threads: return
        w_map = {'daq':(DaqWorker,True),'radon':(RadonWorker,False),'magnetometer':(MagnetometerWorker,True),'th_o2':(ThO2Worker,False),'arduino':(ArduinoWorker,False)}
        if name not in w_map: return
        WClass, use_run = w_map[name]; thread = QThread(); worker = WClass(self.config.get(name, {}), self.db_queue)
        worker.moveToThread(thread)
        if hasattr(worker, 'error_occurred'): worker.error_occurred.connect(self.show_error)
        
        # 라돈 워커의 상태 신호는 전용 슬롯에, 나머지는 상태 표시줄에 연결
        if name == 'radon' and hasattr(worker, 'radon_status_update'):
            worker.radon_status_update.connect(self._update_radon_status)
        elif hasattr(worker, 'status_update'):
            worker.status_update.connect(self.status_bar.showMessage)
        
        s_map = {'daq':'avg_data_ready','radon':'data_ready','magnetometer':'avg_data_ready','th_o2':'avg_data_ready','arduino':'avg_data_ready'}
        slot_map = {'daq':self.update_daq_ui,'radon':self.update_radon_ui,'magnetometer':self.update_mag_ui,'th_o2':self.update_th_o2_ui,'arduino':self.update_arduino_ui}
        if name in s_map and hasattr(worker, s_map[name]): getattr(worker, s_map[name]).connect(slot_map[name])
        if name in ['daq', 'magnetometer', 'th_o2', 'arduino'] and hasattr(worker, 'raw_data_ready'): worker.raw_data_ready.connect(self.update_raw_ui)
        
        thread.started.connect(worker.run if use_run else worker.start_worker)
        thread.start()
        self.threads[name] = (thread, worker)

    def _stop_worker(self, name):
        if name in self.threads:
            thread,worker=self.threads.pop(name)
            stop_method = getattr(worker, 'stop', getattr(worker, 'stop_worker', None))
            if stop_method: stop_method()
            thread.quit(); thread.wait(3000)
    
    def _convert_daq_voltage_to_distance(self, v, mapping_index):
        try:
            volt_module = next((mod for mod in self.config['daq']['modules'] if mod['task_type'] == 'volt'), None)
            if volt_module:
                m = volt_module['mapping'][mapping_index]
                v_min, v_max = m['volt_range']; d_min, d_max = m['dist_range_mm']
                return d_min + ((v - v_min) / (v_max - v_min)) * (d_max - d_min)
        except (IndexError, KeyError, StopIteration):
            logging.warning(f"Failed to convert voltage to distance: {e}"); return 0.0

    @pyqtSlot(float, dict)
    def update_daq_ui(self, ts, data):
        ptr = self.pointers['daq']; rtd, dist = data.get('rtd', []), data.get('dist', [])
        self.rtd_data[ptr] = [ts, rtd[0] if len(rtd) > 0 else np.nan, rtd[1] if len(rtd) > 1 else np.nan]
        self.dist_data[ptr] = [ts, dist[0] if len(dist) > 0 else np.nan, dist[1] if len(dist) > 1 else np.nan]
        self.pointers['daq'] = (ptr + 1) % self.max_lens['daq']
        self.plot_dirty_flags.update({"daq_ls_temp_L_LS_Temp": True, "daq_ls_temp_R_LS_Temp": True,"daq_ls_level_GdLS Level": True, "daq_ls_level_GCLS Level": True})

    @pyqtSlot(float, float, float)
    def update_radon_ui(self, ts, mu, sigma):
        ptr = self.pointers['radon']; self.radon_data[ptr] = [ts, mu]
        self.pointers['radon'] = (ptr + 1) % self.max_lens['radon']
        self.plot_dirty_flags["radon_Radon (μ)"] = True

    @pyqtSlot(float, list)
    def update_mag_ui(self, ts, mag):
        ptr = self.pointers['mag']; self.mag_data[ptr] = [ts] + mag
        self.pointers['mag'] = (ptr + 1) % self.max_lens['mag']
        self.plot_dirty_flags.update({"mag_Bx": True, "mag_By": True, "mag_Bz": True, "mag_|B|": True})

    @pyqtSlot(float, float, float, float)
    def update_th_o2_ui(self, ts, temp, humi, o2):
        ptr = self.pointers['th_o2']; self.th_o2_data[ptr] = [ts, temp, humi, o2]
        self.pointers['th_o2'] = (ptr + 1) % self.max_lens['th_o2']
        self.plot_dirty_flags.update({"th_o2_temp_humi_Temp(°C)": True, "th_o2_temp_humi_Humi(%)": True, "th_o2_o2_Oxygen(%)": True})

    @pyqtSlot(float, dict)
    def update_arduino_ui(self, ts, data):
        ptr = self.pointers['arduino']
        self.arduino_data[ptr] = [ts, data.get('temp0', np.nan), data.get('humi0', np.nan), data.get('temp1', np.nan), data.get('humi1', np.nan), data.get('dist', np.nan)]
        self.pointers['arduino'] = (ptr + 1) % self.max_lens['arduino']
        self.plot_dirty_flags.update({"arduino_temp_humi_T1(°C)": True, "arduino_temp_humi_H1(%)": True,"arduino_temp_humi_T2(°C)": True, "arduino_temp_humi_H2(%)": True,"arduino_dist_Dist(cm)": True})

    @pyqtSlot(dict)
    def update_raw_ui(self, data):
        if 'rtd' in data or 'volt' in data:
            rtd, volt = data.get('rtd', []), data.get('volt', [])
            if len(rtd) > 0: self.latest_raw_values["L_LS_Temp"] = f"L LS Temp: {rtd[0]:.2f} °C"
            if len(rtd) > 1: self.latest_raw_values["R_LS_Temp"] = f"R LS Temp: {rtd[1]:.2f} °C"
            if len(volt) > 0: self.latest_raw_values["GdLS_level"] = f"GdLS level: {self._convert_daq_voltage_to_distance(volt[0], 0):.1f} mm"
            if len(volt) > 1: self.latest_raw_values["GCLS_level"] = f"GCLS level: {self._convert_daq_voltage_to_distance(volt[1], 1):.1f} mm"
        if 'mag' in data:
            mag = data.get('mag', []); keys = ["B_x", "B_y", "B_z", "|B|"]; labels = ["X", "Y", "Z", "|B|"]
            for i, key in enumerate(keys):
                if len(mag) > i: self.latest_raw_values[key] = f"{labels[i]}: {mag[i]:.2f} mG"
        if 'th_o2' in data:
            d = data['th_o2']
            if 'temp' in d: self.latest_raw_values["TH_O2_Temp"] = f"Temp: {d['temp']:.2f} °C"
            if 'humi' in d: self.latest_raw_values["TH_O2_Humi"] = f"Humi: {d['humi']:.2f} %"
            if 'o2' in d: self.latest_raw_values["TH_O2_Oxygen"] = f"Oxygen: {d['o2']:.2f} %"
        if 'arduino' in data:
            d = data['arduino']
            if 'temp0' in d and d['temp0'] is not None: self.latest_raw_values["Arduino_Temp1"] = f"Temp1: {d['temp0']:.2f} °C"
            if 'humi0' in d and d['humi0'] is not None: self.latest_raw_values["Arduino_Humi1"] = f"Humi1: {d['humi0']:.2f} %"
            if 'temp1' in d and d['temp1'] is not None: self.latest_raw_values["Arduino_Temp2"] = f"Temp2: {d['temp1']:.2f} °C"
            if 'humi1' in d and d['humi1'] is not None: self.latest_raw_values["Arduino_Humi2"] = f"Humi2: {d['humi1']:.2f} %"
            if 'dist' in d and d['dist'] is not None: self.latest_raw_values["Arduino_Dist"] = f"Dist: {d['dist']:.1f} cm"

    @pyqtSlot()
    def _update_gui(self):
        dirty_keys = [key for key, dirty in self.plot_dirty_flags.items() if dirty]
        if dirty_keys:
            for curve_key in dirty_keys:
                if curve_key in self.curves and curve_key in self.curve_data_map:
                    x_data, y_data = self.curve_data_map[curve_key]
                    self.curves[curve_key].setData(x=x_data, y=y_data, connect='finite')
            self.plot_dirty_flags.clear()
        for key, text in self.latest_raw_values.items():
            if key in self.labels: self.labels[key].setText(text)
        self.latest_raw_values.clear()

    def show_error(self, msg):
        logging.error(f"GUI Error: {msg}")
        if QThread.currentThread() is self.thread():
            QMessageBox.critical(self, "Error", msg)
        else:
            QTimer.singleShot(0, lambda: QMessageBox.critical(self, "Error", msg))

    def closeEvent(self, event):
        logging.info("Application closing...")
        active_threads = list(self.threads.keys())
        for name in active_threads:
            thread, worker = self.threads[name]
            stop_method = getattr(worker, 'stop', getattr(worker, 'stop_worker', None))
            if stop_method: QMetaObject.invokeMethod(worker, stop_method.__name__, Qt.QueuedConnection)
        for name in active_threads:
            thread, worker = self.threads[name]
            if not thread.wait(5000): logging.warning(f"{name} worker thread did not finish cleanly.")
        if hasattr(self, 'hw_thread'):
            QMetaObject.invokeMethod(self.hw_manager, "stop_scan", Qt.QueuedConnection)
            self.hw_thread.quit()
            if not self.hw_thread.wait(3000): logging.warning("HardwareManager thread did not finish cleanly.")
        logging.info("All threads stopped. Exiting.")
        event.accept()

# ====================================================================================
# Main Entry Point
# ====================================================================================
if __name__ == '__main__':
    load_config()
    log_level = CONFIG.get('logging_level', 'INFO').upper()
    log_filename = "rene_pm.log"
    with open(log_filename, 'w'):
        pass
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), 
                        format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s', 
                        handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])
    
    logging.info("="*50 + "\nRENE-PM Integrated Monitoring System Starting\n" + "="*50)
    
    app = QApplication(sys.argv)
    
    signal.signal(signal.SIGINT, lambda s, f: QApplication.quit())
    timer = QTimer()
    timer.start(500)
    timer.timeout.connect(lambda: None)

    main_win = MainWindow(config=CONFIG)
    main_win.show()
    sys.exit(app.exec_())