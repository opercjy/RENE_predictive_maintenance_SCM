import sys
import time
import random
import os
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QGridLayout, QLabel, QScrollArea, QGraphicsScene, QGraphicsView, QGraphicsProxyWidget, QGraphicsPathItem  
from PyQt5.QtCore import Qt, QTimer, QRectF
from PyQt5.QtGui import QColor, QFont, QTextCursor, QPixmap, QPainterPath, QBrush
from caen_libs import caenhvwrapper as hv # caen_hv_py -> caenhvwrapper
import mariadb


# Crate Map 정보
crate_map = {
    1: {'model': 'A7030P', 'channels': 48},
    4: {'model': 'A7435SN', 'channels': 24},
    8: {'model': 'A7435SN', 'channels': 24},
}
# Display Map 정보 (GUI에 표시할 채널)
display_map = {
    1: [0, 1],  # Slot 1: ch 1, 2만 표시
    4: sorted(random.sample(range(24), 18)),  # Slot 4: 18개 랜덤 채널
    8: [num for num in range(24) if 8 <= num <= 23],  # Slot 8: 8번부터 23번까지 16개 채널
}
# Label Positions 정보 (GUI에 표시할 채널의 캔버스 좌표)
label_positions = {
    (1, 0): (613, 610),   # Slot 1, Channel 1
    (1, 1): (1130, 610),   # Slot 1, Channel 2
    (4, display_map[4][0]): (410, 546),   # Slot 4, Channel display_map[4][0] 
    (4, display_map[4][1]): (1324, 538),   # Slot 4, Channel display_map[4][1]
    (4, display_map[4][2]): (406, 454),   # Slot 4, Channel display_map[4][2] 
    (4, display_map[4][3]): (1324, 450),   # Slot 4, Channel display_map[4][3]
    (4, display_map[4][4]): (410, 358),   # Slot 4, Channel display_map[4][4] 
    (4, display_map[4][5]): (1329, 356),   # Slot 4, Channel display_map[4][5]
    (4, display_map[4][6]): (320, 120),   # Slot 4, Channel display_map[4][6] 
    (4, display_map[4][7]): (1400, 116),   # Slot 4, Channel display_map[4][7]
    (4, display_map[4][8]): (312, 175),   # Slot 4, Channel display_map[4][8] 
    (4, display_map[4][9]): (1412, 170),   # Slot 4, Channel display_map[4][9]
    (4, display_map[4][10]): (302, 230),   # Slot 4, Channel display_map[4][10] 
    (4, display_map[4][11]): (1444, 224),   # Slot 4, Channel display_map[4][11]
    (4, display_map[4][12]): (305, 687),   # Slot 4, Channel display_map[4][12] 
    (4, display_map[4][13]): (1420, 676),   # Slot 4, Channel display_map[4][13]    
    (4, display_map[4][14]): (310, 814),   # Slot 4, Channel display_map[4][14] 
    (4, display_map[4][15]): (1410, 804),  # Slot 4, Channel display_map[4][15]
    (4, display_map[4][16]): (320, 926),   # Slot 4, Channel display_map[4][16] 
    (4, display_map[4][17]): (1400, 920),  # Slot 4, Channel display_map[4][18]
    (8, display_map[8][0]): (200, 320),
    (8, display_map[8][1]): (60, 380), 
    (8, display_map[8][2]): (90, 522), 
    (8, display_map[8][3]): (36, 580), 
    (8, display_map[8][4]): (90, 640), 
    (8, display_map[8][5]): (36, 700), 
    (8, display_map[8][6]): (90, 760), 
    (8, display_map[8][7]): (36, 820), 
    (8, display_map[8][8]): (1510, 320), 
    (8, display_map[8][9]): (1660, 380), 
    (8, display_map[8][10]): (1622, 520), 
    (8, display_map[8][11]): (1688, 580), 
    (8, display_map[8][12]): (1622, 640), 
    (8, display_map[8][13]): (1688, 700), 
    (8, display_map[8][14]): (1622, 760), 
    (8, display_map[8][15]): (1688, 820), 
}

# MariaDB 커넥션 풀 설정
pool =  mariadb.ConnectionPool(
    user="SCM_HV_ADMIN",
    password="",
    host="localhost",
    port=3306,
    database="SCM_HV",
    pool_name="hv_monitor_pool",
    unix_socket="/home/mariadb_data/mysql/mysql.sock",
    pool_size=5  # 풀 크기 설정
)

# 저장 프로시저 및 테이블 생성 (프로그램 실행 초기에 한 번만 실행)
def initialize_database():
    try:
        conn = pool.get_connection()
        cur = conn.cursor()

        # 저장 프로시저 생성 쿼리문
        create_procedure_query = """
        CREATE PROCEDURE IF NOT EXISTS get_hv_data()
        BEGIN
          DECLARE i INT DEFAULT 1;
          DECLARE j INT DEFAULT 0;
          DECLARE max_slot INT DEFAULT 8;
          DECLARE max_channel INT;

          CREATE TEMPORARY TABLE IF NOT EXISTS temp_hv_data (
            time INT,
            label VARCHAR(255),
            vmon FLOAT
          );

          TRUNCATE TABLE temp_hv_data;

          WHILE i <= max_slot DO
            IF i = 1 THEN
              SET max_channel = 47;
            ELSE
              SET max_channel = 23;
            END IF;

            SET j = 0;
            WHILE j <= max_channel DO
              INSERT INTO temp_hv_data
              SELECT 
                UNIX_TIMESTAMP(datetime), 
                CONCAT('slot ', i, ', channel ', j),
                vmon
              FROM SCM_HV_TEST_DB.TEST_HV 
              WHERE slot = i AND channel = j AND datetime >= DATE(NOW()) - INTERVAL 7 DAY;

              SET j = j + 1;
            END WHILE;

            SET i = i + 1;
          END WHILE;
        END 
        """
        cur.execute(create_procedure_query)
        conn.commit()

        # 테이블 생성 쿼리
        create_table_query = """
        CREATE TABLE IF NOT EXISTS HV_DATA (
           datetime DATETIME,
           slot INT,
           channel INT,
           power INT,
           poweron INT,
           powerdown INT,
           vmon FLOAT,
           imon FLOAT,
           v0set FLOAT,
           i0set FLOAT,
           PRIMARY KEY (datetime, slot, channel)
        )
        """
        cur.execute(create_table_query)
        conn.commit()

    except Exception as e:
        print(f"Error initializing database: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

initialize_database()  # 프로그램 시작 시 저장 프로시저 및 테이블 생성


class HVMonitor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("RENE HV Monitor (Selected)")
        # caenhvwrapper Device 객체 생성
        try:
            self.device = hv.Device.open(hv.SystemType.SY4527, hv.LinkType.TCPIP, '192.168.0.39', 'admin', 'admin')
        except hv.Error as e:
            print(f"Error connecting to CAEN HV system: {e}")
            sys.exit(1)
        """
        # PowerChute 이벤트 감시 타이머 설정, apcupsd 데몬을 설치하면 전원 관련된 환경 변수를 설정함
        self.powerchute_timer = QTimer()
        self.powerchute_timer.timeout.connect(self.check_power_event)
        self.powerchute_timer.start(10000)  # 1초마다 이벤트 확인

    def check_power_event(self):
        #PowerChute 환경 변수를 확인하여 전원 이벤트 발생 시 HV 전원을 끕니다.
        try:
            power_event = os.environ.get('POWER_EVENT')
            # POWER_EVENT 환경 변수가 설정되어 있는지 확인
            if power_event is None:
                print("POWER_EVENT environment variable is not set.")
                # POWER_EVENT 환경 변수가 설정되어 있지 않으면 타이머를 중지하고 함수 종료
                self.powerchute_timer.stop()
                return

            if power_event == "1":
                self.power_off_hv()
                # 이벤트 처리 후 환경 변수 초기화 (필요에 따라)
                os.environ['POWER_EVENT'] = "0" 
        except Exception as e:
            print(f"Error checking power event: {e}")

    def power_off_hv(self):
        #모든 HV 채널의 전원을 끕니다.
        try:
            for slot, board_info in crate_map.items():
                for channel in range(board_info['channels']):
                    self.device.set_ch_param(slot, [channel], 'Pw', [0])
            print("Powering off all HV channels due to power event.")
        except hv.Error as e:
            print(f"Error powering off HV: {e}")

        """
        # QHD 해상도의 1/4 크기 계산
        screen_geometry = QApplication.desktop().screenGeometry()
        window_width = screen_geometry.width() 
        window_height = screen_geometry.height() 
        self.resize(window_width, window_height)
        # 스크롤 가능한 영역 생성
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        self.setCentralWidget(scroll_area)
        # 레이아웃 설정
        widget = QWidget()
        grid_layout = QGridLayout()
        widget.setLayout(grid_layout)
        scroll_area.setWidget(widget)
        # 캔버스 설정
        self.scene = QGraphicsScene()
        self.view = QGraphicsView(self.scene)
        self.view.setFixedSize(1920, 1080)  # 또는 원하는 크기로 설정

        # 범례 라벨 생성
        self.legend_label = QLabel()
        self.legend_label.setAlignment(Qt.AlignLeft)
        self.legend_label.setStyleSheet("background-color: rgba(0, 0, 0, 58); color: white;")
        self.legend_label.setFixedSize(250, 220)

        # 범례 라벨 텍스트 설정 (HTML 사용, 텍스트 중앙 정렬)
        legend_text = """
            <p style='font-size: 12pt; margin: 2px; text-align: center;'><b>범례</b></p>
            <p style='font-size: 10pt; margin: 2px; text-align: center;'><b>전압 (도형색)</b></p>
            <p style='font-size: 10pt; margin: 2px; text-align: center;'><span style='background-color: green; color: black;'>●</span> <span>|Vmon - V0Set| ≤ 10 V</span></p>
            <p style='font-size: 10pt; margin: 2px; text-align: center;'><span style='background-color: yellow; color: black;'>●</span> <span>10 V &lt; |Vmon - V0Set| ≤ 30 V</span></p>
            <p style='font-size: 10pt; margin: 2px; text-align: center;'><span style='background-color: magenta; color: black;'>●</span> <span>30 V &lt; |Vmon - V0Set| ≤ 50 V</span></p>
            <p style='font-size: 10pt; margin: 2px; text-align: center;'><span style='background-color: red; color: black;'>●</span> <span>50 V &lt; |Vmon - V0Set|</span></p>
            <p style='font-size: 10pt; margin: 2px; text-align: center;'><b>전류 (글자색)</b></p>
            <p style='font-size: 12pt; margin: 2px; text-align: center;'><span style='color: yellow;'>●</span> <span style='color: black;'>IMon ≥ 0 uA</span></p> 
            <p style='font-size: 12pt; margin: 2px; text-align: center;'><span style='color: cyan;'>●</span> <span style='color: white;'>IMon &lt; 0 uA</span></p> 
        """
        self.legend_label.setText(legend_text)

        # 범례 라벨을 캔버스에 추가
        legend_label_item = QGraphicsProxyWidget()
        legend_label_item.setWidget(self.legend_label)
        legend_label_item.setPos(1650, 100)  # "Logging and Monitoring" 라벨 아래에 배치 (필요에 따라 조정)
        self.scene.addItem(legend_label_item)

        # 현재 시각 표시 라벨 생성 및 캔버스에 추가
        self.time_label = QLabel()
        self.time_label.setAlignment(Qt.AlignCenter)
        self.time_label.setStyleSheet("background-color: black; color: white;")
        self.time_label.setFixedSize(250, 50)  # 라벨 크기 조정 (필요에 따라 값 변경)

        time_label_item = QGraphicsProxyWidget()
        time_label_item.setWidget(self.time_label)
        time_label_item.setPos(1650, 50)  # 우측 상단에 배치 (필요에 따라 조정)
        
        # Logging and Monitoring 폰트 크기 조정
        font = QFont("Noto Sans", 12)  # 폰트 크기 14로 설정
        self.time_label.setFont(font)

        self.scene.addItem(time_label_item)
        
        # 현재 시각 업데이트 타이머 설정
        self.time_timer = QTimer()
        self.time_timer.timeout.connect(self.update_time)
        self.time_timer.start(1000)  # (단위 ms) 1 초마다 업데이트
        # 도면 이미지 로드 및 배경 설정 (실제 이미지 경로로 수정)
        pixmap = QPixmap("RENE_VETO_FOR_HV4.png")  # 또는 배경 없이 사용하려면 주석 처리
        self.scene.addPixmap(pixmap)
        # 채널 정보 표시 라벨 및 둥근 모서리 사각형 생성 및 캔버스에 추가
        self.labels = {}
        self.label_items = {}
        self.shape_items = {}
        for (slot, channel) in sorted(label_positions.keys()):
            label = QLabel()  # 빈 QLabel 생성
            label.setAlignment(Qt.AlignCenter)
            self.labels[(slot, channel)] = label
            # 라벨을 캔버스 아이템으로 변환
            label_item = QGraphicsProxyWidget()
            label_item.setWidget(label)
            x, y = label_positions[(slot, channel)]
            label_item.setPos(x, y)
            self.label_items[(slot, channel)] = label_item
            # slot 1, ch 1, 2의 경우 타원, 그 외에는 둥근 모서리 사각형 생성
            if slot == 1 and channel in [0, 1]:
                # 타원 경로 생성
                path = QPainterPath()
                ellipse_rect = QRectF(0, 0, 160, 80)  # 타원의 경계 사각형
                path.addEllipse(ellipse_rect)
            else:
                # 둥근 모서리 사각형 경로 생성
                path = QPainterPath()
                path.addRoundedRect(QRectF(0, 0, 160, 50), 10, 10)
            shape_item = QGraphicsPathItem(path)
            shape_item.setPos(x, y)
            self.shape_items[(slot, channel)] = shape_item  # rect_items -> shape_items
            # 도형을 먼저 추가하고, 라벨을 나중에 추가하여 텍스트가 위에 오도록 합니다.
            self.scene.addItem(shape_item)
            self.scene.addItem(label_item) 
        # 레이아웃에 캔버스 추가
        grid_layout.addWidget(self.view, 0, 0)

        # 데이터 업데이트 타이머 설정 (GUI 업데이트용)
        self.gui_timer = QTimer()
        self.gui_timer.timeout.connect(self.update_gui)  # GUI 업데이트 함수 연결
        self.gui_timer.start(1000)  # 1초마다 업데이트
        # 데이터베이스 업데이트 타이머 설정 (DB 커밋용)
        self.db_timer = QTimer()
        self.db_timer.timeout.connect(self.update_database)  # DB 업데이트 함수 연결
        self.db_timer.start(60000)  # 60초마다 업데이트
        self.last_db_update = 0

    def update_time(self):  # HVMonitor 클래스 내부에 정의
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.setText(f"Logging and Monitoring\n{current_time}")
        #print(f"Updated time_label text: {self.time_label.text()}")  # 디버깅용 출력

    def update_gui(self):  # GUI 업데이트 함수
        try:
            for slot, board_info in crate_map.items():
                for channel in range(board_info['channels']):
                    power = self.device.get_ch_param(slot, [channel], 'Pw')[0] 
                    poweron = self.device.get_ch_param(slot, [channel], 'POn')[0] 
                    powerdown = self.device.get_ch_param(slot, [channel], 'PDwn')[0] 
                    vmon = self.device.get_ch_param(slot, [channel], 'VMon')[0] 
                    imon = self.device.get_ch_param(slot, [channel], 'IMon')[0]
                    v0set = self.device.get_ch_param(slot, [channel], 'V0Set')[0]
                    i0set = self.device.get_ch_param(slot, [channel], 'I0Set')[0]
                    #timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    # GUI에 표시할 채널만 업데이트
                    if (slot, channel) in self.labels:
                        label = self.labels[(slot, channel)]
                        font_size = 12
                        if power == 0:
                            text = f"""<p style='font-size:{font_size}pt;line-height: 0.7; margin: 0px; text-align: center;'>Slot{slot}, Ch{channel}</p>
<p style='font-size:{font_size}pt;line-height: 0.7; margin: 0px; text-align: center;'>Power Off</p>""" 
                            color = "gray"
                            text_color = "white"
                        else:
                            # 텍스트를 여러 줄로 표시하고 Vmon 강조, 줄 간격 조정
                            text = f"""<p style="line-height: 0.7; margin: 0px; font-size:{font_size}pt;">Slot{slot}, Ch{channel}</p>
<p style="line-height: 0.7; margin: 0px;"><b><span style="font-size:12pt;">{vmon:.1f} V, {imon:.2f} uA</span></b></p>""" 
                            color = self.vmon_to_color(vmon, v0set)
                            text_color = "black" if imon >= 0 else "white"
                           # 캔버스 아이템의 라벨 업데이트
                        label_item = self.label_items[(slot, channel)]
                        label_item.setWidget(label) 
                        label.setText(text)
                        label.setStyleSheet(f"background-color: transparent; color: {text_color};")  # 배경 투명하게 설정
                        # 라벨 내부 여백(padding) 줄이기
                        label.setContentsMargins(0, 0, 0, 0)
                        # 고해상도 폰트 설정 (예: Noto Sans)
                        font = QFont("Noto Sans", font_size) 
                        label.setFont(font)  
                        # 텍스트 크기 자동 조절 (HTML 태그 제외)
                        self.adjust_label_font_size(label, text)
                        # 둥근 모서리 사각형 색상 및 투명도 업데이트 (PyQt5 5.12 이전 버전)
                        shape_item = self.shape_items[(slot, channel)]
                        alpha = 178  # 투명도 설정 (0 ~ 255 범위, 128은 약 50% 투명)
                        color_with_alpha = QColor(color)
                        color_with_alpha.setAlpha(alpha)
                        brush = QBrush(color_with_alpha)
                        brush.setStyle(Qt.SolidPattern)
                        if color != "black":  # 검정색은 투명하게 설정하지 않음
                            shape_item.setBrush(brush)
        except hv.Error as e:
            print(f"Error updating GUI: {e}")

    def update_database(self):  # DB 업데이트 함수
        try:
            conn = pool.get_connection()  # 커넥션 풀에서 연결 가져오기
            cur = conn.cursor()
            data_to_insert = []  # 삽입할 데이터를 저장할 리스트
            for slot, board_info in crate_map.items():
                for channel in range(board_info['channels']):
                    # 모든 채널 데이터 가져오기 (DB 저장용)
                    power = self.device.get_ch_param(slot, [channel], 'Pw')[0] 
                    poweron = self.device.get_ch_param(slot, [channel], 'POn')[0] 
                    powerdown = self.device.get_ch_param(slot, [channel], 'PDwn')[0] 
                    vmon = self.device.get_ch_param(slot, [channel], 'VMon')[0] 
                    imon = self.device.get_ch_param(slot, [channel], 'IMon')[0]
                    v0set = self.device.get_ch_param(slot, [channel], 'V0Set')[0]
                    i0set = self.device.get_ch_param(slot, [channel], 'I0Set')[0]
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    data_to_insert.append((timestamp, slot, channel, power, poweron, powerdown, vmon, imon, v0set, i0set))
                    # 
                    try:
                        with conn.cursor() as cur:  # with 문 사용하여 커서 자동 닫기
                            cur.executemany("""
                            INSERT IGNORE INTO HV_DATA (datetime, slot, channel, power, poweron, powerdown, vmon, imon, v0set, i0set)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, data_to_insert)
                            conn.commit()  # 변경 사항 커밋 
                    except Exception as e:
                        print(f"Error committing data: {e}")      

        except hv.Error as e:
            print(f"Error updating data: {e}")

        finally:
            if conn:
                cur.close()
                conn.close()  # 연결을 풀에 반환

    def closeEvent(self, event):
        # 프로그램 종료 시 디바이스 연결 닫기
        self.device.close()        
        # 프로그램 종료 시 데이터 베이스 연결 닫기 
        # self.cur.close() # pool 에서 자동 관리 해서 주석처리
        # self.conn.close() # pool 에서 자동 관리 해서 주석처리
        super().closeEvent(event)        

    def adjust_label_font_size(self, label, html_text):
        font = label.font()
        font_metrics = label.fontMetrics()
        # 텍스트의 내용을 줄 단위로 분리 (HTML 태그 제외)
        lines = [line.strip() for line in html_text.split("<p>") if line.strip()]
        # 각 줄의 너비 중 최대값 계산
        max_line_width = max(font_metrics.boundingRect(line).width() for line in lines)
        # 폰트 크기 조절
        while (max_line_width > label.width() or font_metrics.height() * len(lines) > label.height()) and font.pointSize() > 5:
            font.setPointSize(font.pointSize() - 1)
            font_metrics = label.fontMetrics()
            max_line_width = max(font_metrics.boundingRect(line).width() for line in lines)
        label.setFont(font)

    def vmon_to_color(self, vmon, v0set):
      if abs(vmon-v0set) <= 10:
        return "green" 
      elif 10 < abs(vmon-v0set) <= 30:
        return "yellow"
      elif 30 < abs(vmon-v0set) <= 50:
        return "magenta"
      else:
        return "red" 
      
if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = HVMonitor()
    window.show()
    sys.exit(app.exec_())

print('HV mon/push script was interrupted')
