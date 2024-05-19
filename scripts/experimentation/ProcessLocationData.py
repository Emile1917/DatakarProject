import sys
import os
import pandas as pd
import polars as pl
import dask.dataframe as dd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

sys.path.append('./')
sys.path.append('./scripts/connection')

from scripts.connection.Req import Req

class ProcessLocationData:
    def __init__(self):
        self.departments_path = "./data/externals/french-departments-geolocation.csv"
        self.meeting_location_path = "./data/tables/meeting_points.csv"
        self.partnership_path = 'partnerships.csv'
        self.test_dir = './data/tests/'
        self.current_time = datetime.now()
        self.req = Req()

    def process_departments(self):
        departments = pd.read_csv(self.departments_path)
        departments.rename(columns={
            "Latitude la plus au nord": "Nothernmost_latitude",
            "Latitude la plus au sud": "Southernmost_latitude",
            "Longitude la plus à l’ouest": "Furthestwest_longitude",
            "Longitude la plus à l’est": "Furthesteast_longitude"
        }, inplace=True)
        return departments

    def get_list_localisation(self):
        meeting_locations = pd.read_csv(self.meeting_location_path)
        latitude = meeting_locations['mp_latitude'].to_list()
        longitude = meeting_locations['mp_longitude'].to_list()
        return list(zip(latitude, longitude))

    def get_list_departements(self):
        departments = self.process_departments()
        lat_north = departments["Nothernmost_latitude"].to_list()
        lat_south = departments["Southernmost_latitude"].to_list()
        lon_east = departments["Furthesteast_longitude"].to_list()
        lon_west = departments["Furthestwest_longitude"].to_list()
        dep = departments["Departement"].to_list()
        return list(zip(lat_north, lat_south, lon_east, lon_west, dep))

    def geo_comparison(self, row, locals, bornSup0, bornInf0, bornSup1, bornInf1, value):
        returnValue = ''
        for l in locals:
            if float(l[0]) < float(row[bornSup0]) and float(l[0]) > float(row[bornInf0]) and float(l[1]) < float(row[bornSup1]) and float(l[1]) > float(row[bornInf1]):
                returnValue = row[value]
        return returnValue

    def get_localisations(self, row, lat_name, lon_name):
        return self.req.get_department(row[lat_name], row[lon_name])

    def get_localisation(self, row):
        return self.req.get_department(row["mp_latitude"], row["mp_longitude"])

    def process_department_meetingLocations(self):
        begin_time = datetime.now()
        meeting_locations = pd.read_csv(self.meeting_location_path)
        meeting_locations["departement"] = meeting_locations.apply(self.get_localisations, args=["mp_latitude", "mp_longitude"], axis=1)
        print(datetime.now() - begin_time)

    def processdask_department_meetingLocation(self, df):
        ddf = dd.from_pandas(df, npartitions=100)
        ddf = ddf.assign(departments=df.apply(self.get_localisation, axis=1))
        print(datetime.now() - self.current_time)

    def processpolars_department_meetingLocation(self, df):
        pldf = pl.DataFrame._from_pandas(df)
        pldf = pldf.with_columns(calc_data=pl.Series(self.get_localisation(x) for x in pldf.iter_rows(named=True)))
        print(datetime.now() - self.current_time)

    def custom_function(self, lat_name, lon_name):
        return self.req.get_department(lat_name, lon_name)

    def spark_init(self):
        return SparkSession.builder.appName("Pandas to Spark").getOrCreate()

    def processspark_department_meetingLocation(self, path):
        spark = self.spark_init()
        df = spark.read.csv(path, header=True)
        upperCaseUDF = udf(lambda x, y: self.custom_function(x, y), StringType())
        df = df.withColumn("departement", upperCaseUDF("mp_latitude", "mp_longitude"))
        print(datetime.now() - self.current_time)

    def geolocalisation(self):
        departs = []
        locals = self.get_list_localisation()
        deps = self.get_list_departements()
        for l in locals:
            for d in deps:
                if float(l[0]) < float(d[0]) and float(l[0]) > float(d[1]) and float(l[1]) < float(d[2]) and float(l[1]) > float(d[3]):
                    departs.append(d[4])
                    break

        meeting_locations = pd.read_csv(self.meeting_location_path)
        meeting_locations['department'] = departs
        meeting_locations.to_csv(self.test_dir + 'department_meetings.csv', index=False)

    def checking_date(self, start_date, end_date):
        try:
            date_format = "%Y-%m-%d %H:%M:%S"
            start_date_object = datetime.strptime(start_date, date_format).date()
            end_date_object = datetime.strptime(end_date, date_format).date()
            if start_date_object <= end_date_object:
                return True
            else:
                return False
        except:
            print("veuillez revoir le format de date 'année-mois-jour heure:minute:seconde' ")

    def sub_lists(self, list1, list2):
        return [l for l in list1 if l in list2]

    def get_partnerships(self):
        partnerships = []
        if os.path.exists(self.partnership_path):
            df = pd.read_csv(self.partnership_path)
            partnerships = list(df['partnership_type'])
        return partnerships

    def process_dept_by_partners(self, partnership_types, start_date, end_date):
        partners = self.get_partnerships()
        partnership_types = self.sub_lists(partnership_types, partners)
        check_date = self.checking_date(start_date, end_date)
        if not check_date:
            print("veuillez changer la date de début et celle de fin, car celle de fin ne peut être plus petite que celle du début")
            return

        if len(partnership_types) == 0:
            print("veuillez vérifier les partenaires")
            return

        lesson_path = "./data/tables/lessons.csv"
        meeting_dept_path = "./data/tests/meetings_departements.csv"
        instructor_path = "./data/tables/instructors.csv"
        booking_path = "./data/tables/bookings.csv"

        spark = self.spark_init()
        instructors = spark.read.csv(instructor_path, header=True)
        bookings = spark.read.csv(booking_path, header=True)
        lessons = spark.read.csv(lesson_path, header=True)
        meetings = spark.read.csv(meeting_dept_path, header=True)

        lessons = lessons.filter((lessons.lesson_deleted_at.isNull()) & (lessons.lesson_created_at.isNotNull()))
        lessons = lessons.select("Lesson_id", "Instructor_id", "Meeting_point_id").where(lessons.Lesson_start_at.between(start_date, end_date))

        instructors = instructors.filter(instructors.partnership_type.isin(partnership_types))
        lessons = lessons.join(instructors, "Instructor_id")

        bookings = bookings.filter((bookings.Booking_created_at.isNotNull()) & (bookings.Booking_deleted_at.isNull()))
        bookings = bookings.select("Lesson_id")
        lessons = lessons.join(bookings, "Lesson_id")

        lessons = lessons.select("Lesson_id", "meeting_point_id")
        meetings = meetings.select("meeting_point_id", "departement")
        meetings_lessons = lessons.join(meetings, "meeting_point_id")

        departments = meetings_lessons.select("departement")
        nb_dep = departments.count()
        dep_counts = departments.groupBy("departement").count().withColumnRenamed("count", "number_lessons")
        dep_counts.show(n=dep_counts.count())
        print("Le nombre de lessons concerné est : " + str(nb_dep))


if __name__ == "__main__":
    processor = ProcessLocationData()
    start_date = "2020-01-01 00:00:00"
    end_date = "2021-12-27 00:00:00"
    partners = ["SAS", "SARL", "EURL", "SASU", "ME", "EI", "EIRL"]
    processor.process_dept_by_partners(partners, start_date, end_date)
