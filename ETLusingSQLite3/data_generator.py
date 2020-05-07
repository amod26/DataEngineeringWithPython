from faker import Faker
from functools import reduce
from urllib.parse import parse_qs
from urllib.parse import urlparse

import datetime
import http.server
import json
import logging
import random
import re
import sqlite3
import sys
import _thread
import time
import uuid

class LearnData:
    LESSON_TITLES = [
        "Use Fetch",
        "Communicating with the Server",
        "JS Fundamentals: Objects",
        "JS Fundamentals: Objects Lab",
        "JS Fundamentals: Traversing Nested Objects",
        "JS Fundamentals: Object Iteration",
        "JS Fundamentals: Looping Code Along",
        "Fetch Lab",
        "Asynchrony",
        "Sending Data with Fetch Lab"
    ]

    def __init__(self, interval_ms=1000):
        logging.basicConfig(level=logging.INFO)
        self.interval = interval_ms * 0.001
        self.fake = Faker()

    def seed(self):
        conn = sqlite3.connect("data/.internal.db")
        conn.execute('''CREATE TABLE IF NOT EXISTS batches
        (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, created_at DATETIME NOT NULL)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS lessons
        (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, test_count INTEGER NOT NULL, created_at DATETIME NOT NULL)''')

        self.create_lessons(conn)

        conn.close()

    def run(self):
        self.seed()

        _thread.start_new_thread(self.stream_students, ())
        _thread.start_new_thread(self.stream_batches, ())
        _thread.start_new_thread(self.batch_server, ())

        time.sleep(self.interval) # FIXME: hack to let students and batches populate

        _thread.start_new_thread(self.stream_lesson_test_runs, ())

        while True:
            pass

    def create_lessons(self, conn):
        (count,) = conn.execute("SELECT COUNT(*) FROM lessons").fetchone()
        if count == 0:
            for i, title in enumerate(self.LESSON_TITLES):
                test_count = random.randint(1,11)
                conn.execute(f"INSERT INTO lessons (title, test_count, created_at) VALUES ('{title}', {test_count}, '{datetime.datetime.utcnow()}')")
                conn.commit()

    def random_batch(self, conn):
        (count,) = conn.execute("SELECT COUNT(*) FROM batches").fetchone()
        offset = random.randint(0, count-1)
        row = conn.execute(f"SELECT * FROM batches LIMIT {offset}, 1").fetchone()
        return Batch(*row)

    def random_student(self, conn):
        (count,) = conn.execute("SELECT COUNT(*) FROM students").fetchone()
        offset = random.randint(0, count-1)
        row = conn.execute(f"SELECT * FROM students LIMIT {offset}, 1").fetchone()
        return Student(*row)

    def random_lesson(self, conn):
        (count,) = conn.execute("SELECT COUNT(*) FROM lessons").fetchone()
        offset = random.randint(0, count-1)
        row = conn.execute(f"SELECT * FROM lessons LIMIT {offset}, 1").fetchone()
        return Lesson(*row)

    def stream_lesson_test_runs(self):
        conn = sqlite3.connect("data/.internal.db")

        while True:
            f = open('data/test_runs.log', 'a')

            for _ in range(1, random.randint(1, 20)):
                test_run = self.create_test_run(conn)
                logging.info(f"APPEND -- {test_run}")
                f.write(test_run)
                f.write("\n")

            f.close
            time.sleep(self.interval*1)

    def stream_students(self):
        internal_conn = sqlite3.connect("data/.internal.db")
        external_conn = sqlite3.connect("data/students.db")

        internal_conn.execute('''CREATE TABLE IF NOT EXISTS students
        (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, batch_id INTEGER NOT NULL, created_at DATETIME NOT NULL)''')

        external_conn.execute('''CREATE TABLE IF NOT EXISTS students
        (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, batch_id INTEGER NOT NULL, created_at DATETIME NOT NULL)''')

        while True:
            for _ in range(1,5):
                student = self.create_student(internal_conn, external_conn)
            time.sleep(self.interval*5)

    def stream_batches(self):
        conn = sqlite3.connect("data/.internal.db")

        while True:
            batch = self.create_batch(conn)
            time.sleep(self.interval*10)

    def create_test_run(self, conn):
        lesson = self.random_lesson(conn)
        batch = self.random_batch(conn)
        student = self.random_student(conn)

        passing = random.randint(0, lesson.test_count)
        total = lesson.test_count
        result = f"{passing}{random.choice(['/', ' out of '])}{total}"

        test_run = TestRun(lesson, batch, student, result).to_json()
        return test_run

    def create_batch(self, conn):
        name = f"{random.choice(['web', 'ios', 'data_science'])}-{uuid.uuid1()}"
        batch = Batch(None, name)

        try:
            conn.execute(f"INSERT INTO batches (name, created_at) VALUES ('{batch.name}','{datetime.datetime.utcnow()}')")
            conn.commit()
            logging.info(f"CREATE -- {batch}")
        except sqlite3.OperationalError:
            pass

        return batch

    def create_student(self, internal_conn, external_conn):
        student = Student(None, self.fake.name(), None)
        batch = self.random_batch(internal_conn)

        try:
            internal_conn.execute(f"INSERT INTO students (name, batch_id, created_at) VALUES ('{student.name}', {batch.id}, '{datetime.datetime.utcnow()}')")
            internal_conn.commit()

            external_conn.execute(f"INSERT INTO students (name, batch_id, created_at) VALUES ('{student.name}', {batch.id}, '{datetime.datetime.utcnow()}')")
            external_conn.commit()

            logging.info(f"CREATE -- {student}")
        except sqlite3.OperationalError:
            pass

        return student


    def batch_server(self):
        server_address = ('', 8000)
        httpd = http.server.HTTPServer(server_address, BatchRequestHandler)
        httpd.serve_forever()

class BatchRequestHandler(http.server.BaseHTTPRequestHandler):
  def do_GET(self):
    o = urlparse(self.path)
    if o.path == "/batches":
        qs = parse_qs(o.query)

        try:
            after = qs['after'][0]
        except KeyError:
            after = None

        conn = sqlite3.connect("data/.internal.db")

        if after != None:
            dt = datetime.datetime.strptime(after, '%Y-%m-%dT%H:%M:%S.%f')
            batches = list(map(lambda b: Batch(*b).attrs(), conn.execute(f"SELECT * FROM batches WHERE created_at > datetime({dt.timestamp()}, 'unixepoch') ORDER BY created_at LIMIT 100")))
        else:
            batches = list(map(lambda b: Batch(*b).attrs(), conn.execute(f"SELECT * FROM batches ORDER BY created_at LIMIT 100")))

        conn.close()

        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()

        self.wfile.write(bytes(json.dumps(batches), 'UTF-8'))
    else:
        self.send_response(404)
        self.send_header('Content-type','text/html')
        self.end_headers()

    return

class TestRun:
    def __init__(self, lesson, batch, student, result):
        self.lesson = lesson
        self.batch = batch
        self.student = student
        self.result = result

    def to_json(self):
        r = random.randint(1,10)

        if r > 7:
            return f'{{"result": "{self.result}", "student_id", {self.student.id}, "lesson_id": "{self.lesson.id}"}}'
        elif r < 7 and r > 4:
            return f'{{"result": "{self.result}", "student_id", {self.student.id}, "lesson_title": "{self.lesson.title}"}}'
        else:
            return f'{{"result": "{self.result}", "student_id", {self.student.id}, "lesson_id": {self.lesson.id}, "lesson_title": "{self.lesson.title}"}}'


class Batch:
    def __init__(self, _id, name, created_at=None):
        self.id = _id
        self.name = name
        self.created_at = created_at

    def to_json(self):
        return f'{{"id": {self.id}, "name": {self.name}, "created_at": {self.created_at}}}'

    def attrs(self):
        return {"id": self.id, "name": self.name, "created_at": self.created_at}

    def __str__(self):
        return f"batch {self.name}"

class Lesson:
    def __init__(self, _id, title, test_count, created_at=None):
        self.id = _id
        self.title = title
        self.test_count = test_count
        self.created_at = created_at

    def __str__(self):
        return f"lesson {self.id}, {self.title}, {self.test_count}"

class Student:
    def __init__(self, _id, name, batch_id, created_at=None):
        self.id = _id
        self.name = name
        self.batch_id = batch_id

    def __str__(self):
        return f"student {self.name}"

if __name__ == "__main__":
    if len(sys.argv) > 1:
        LearnData(int(sys.argv[1])).run()
    else:
        LearnData(500).run()
