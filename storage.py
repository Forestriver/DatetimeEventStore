import datetime
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
import json

class DatetimeEventStore:

    #Database Initialization
    def __init__(self):
        #Setting the database
        try:
            with open('db_config.json', 'r') as file:
                db_config = json.load(file)

            #Connecting database
            self.conn = connect(
                host=db_config['hostname'],
                database=db_config['db_name'],
                user=db_config['username'],
                password=db_config['password']
            )

            self.cursor = self.conn.cursor()

        except OperationalError(disconnect, memory allocation etc) as err:
            print_psycopg2_exception(err)
            conn = None


    #Storing event
    def store_event(self, *args, **kwargs):

        date = kwargs['at']
        data = kwargs['data']

        #Data insert into db
        values_added = """
            INSERT INTO events(time, event)
            VALUES (%s, %s)
        """

        records = (date.isoformat(), data)

        self.cursor.execute(values_added, records)

        self.conn.commit()

    #Getting event data
    def get_events(self, *args, **kwargs):

        start = kwargs['start']
        end = kwargs['end']

        values_added = """
            SELECT * FROM events
            WHERE time > %s and time < %s;
        """

        records = (start.isoformat(), end isoformat())

        self.cursor.execute(values_added, records)
        self.conn.commit()

        event_records = self.cursor.fetchall()
        return event_records
