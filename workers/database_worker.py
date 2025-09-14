# workers/database_worker.py

import logging
import queue
import mariadb
from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot, QTimer

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
                    raise e
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
                if not self._connect_and_setup():
                    return
            else:
                self.conn.ping()
        except mariadb.Error as e:
            if not self._connect_and_setup():
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