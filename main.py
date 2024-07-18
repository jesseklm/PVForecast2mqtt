import logging
import sqlite3
from datetime import datetime, timedelta
from time import sleep, time

import pytz

from config import config
from mqtt_handler import MqttHandler

__version__ = '1.0.0'


class PVForecast2mqtt:
    def __init__(self):
        self.mqtt_handler = MqttHandler()
        self.sqlite_path = config['sqlite_path']
        self.timezone = pytz.timezone(config.get('timezone', 'Europe/Berlin'))

    def loop(self):
        while True:
            start_time = time()
            self.read()
            time_taken = time() - start_time
            time_to_sleep = config['update_rate'] - time_taken
            if time_to_sleep > 0:
                sleep(time_to_sleep)

    def read(self):
        with sqlite3.connect(f'file:{self.sqlite_path}?mode=ro', uri=True) as connection:
            cursor = connection.cursor()
            cursor.execute("""
            WITH dwd_ranked AS (
                SELECT PeriodEnd,
                       ac_disc,
                       ROW_NUMBER() OVER (PARTITION BY PeriodEnd ORDER BY IssueTime DESC) as rn,
                       IssueTime
                FROM   dwd
            )
            SELECT UNIXEPOCH(PeriodEnd) AS period_end, ac_disc, UNIXEPOCH(IssueTime) AS issue_time
            FROM dwd_ranked
            WHERE UNIXEPOCH(PeriodEnd) BETWEEN UNIXEPOCH(DATE('now','-0 day')) AND UNIXEPOCH(DATE('now','+1 day'))
            AND   rn = 1
            """)
            table = cursor.fetchall()

        for row in table:
            dt_tz = pytz.utc.localize(datetime.utcfromtimestamp(row[0])).astimezone(self.timezone)
            dt_tz_minus_one_hour = dt_tz - timedelta(hours=1)
            dt_issue_time = pytz.utc.localize(datetime.utcfromtimestamp(row[2])).astimezone(self.timezone)
            if dt_tz_minus_one_hour <= datetime.now(self.timezone) <= dt_tz:
                self.mqtt_handler.publish('now/time', f'{dt_tz_minus_one_hour.hour} - {dt_tz.hour}')
                self.mqtt_handler.publish('now/prediction', f'{row[1]:.0f}')
                self.mqtt_handler.publish('now/issue_time', f'{dt_issue_time}')
                self.mqtt_handler.publish('now/publish_time', f'{datetime.now(self.timezone)}')
            if dt_tz_minus_one_hour <= datetime.now(self.timezone) + timedelta(hours=1) <= dt_tz:
                self.mqtt_handler.publish('next_hour/time', f'{dt_tz_minus_one_hour.hour} - {dt_tz.hour}')
                self.mqtt_handler.publish('next_hour/prediction', f'{row[1]:.0f}')
                self.mqtt_handler.publish('next_hour/issue_time', f'{dt_issue_time}')
                self.mqtt_handler.publish('next_hour/publish_time', f'{datetime.now(self.timezone)}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('pymodbus').setLevel(logging.INFO)
    logging.info(f'starting PVForecast2mqtt v{__version__}.')
    app = PVForecast2mqtt()
    app.loop()
