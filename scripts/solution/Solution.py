import sys
sys.path.append('./')
sys.path.append('./scripts/connection')

from scripts.connection.ConnectionBigQuery import ConnectionBigQuery
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from scripts.connection.Req import Req
import os
import shutil
import pandas as pd
from datetime import datetime

class Solution:
    def __init__(self):
        self.current_time = datetime.now()
        self.partnership_path = 'partnerships.csv'
        self.result_dir = "./data/results/"
        self.spark_write_dir = "departments"

    def get_date_by_instructors(self):
        conn = ConnectionBigQuery()
        QUERY_1 = """
        WITH lesson_booked AS (
        SELECT l.lesson_id 
        FROM `test_dataset.lessons` as l
        INNER JOIN `test_dataset.bookings` as b
        USING (lesson_id)
        WHERE b.booking_created_at IS NOT NULL
        AND b.booking_deleted_at IS NULL
        ),

        lesson_with_instructors AS ( 
        SELECT l.lesson_id,l.instructor_id,l.lesson_start_at
        FROM  `test_dataset.lessons` as l
        INNER JOIN lesson_booked as lb
        ON l.lesson_id = lb.lesson_id
        WHERE l.lesson_created_at IS NOT NULL
        AND l.lesson_deleted_at IS NULL
        AND l.lesson_start_at BETWEEN "2020-07-01" AND "2020-09-30"
        ),

        five_instructors AS (
        SELECT lw.instructor_id , count(lw.lesson_id) AS number_lessons
        FROM lesson_with_instructors as lw
        GROUP BY (lw.instructor_id)
        ORDER BY number_lessons DESC
        LIMIT 5
        ),

        classify_lessons AS (
        SELECT lw.instructor_id , lw.lesson_start_at ,
        ROW_NUMBER() OVER (PARTITION BY  lw.instructor_id ORDER BY lw.lesson_start_at ASC) as course_rank
        FROM lesson_with_instructors as lw
        INNER JOIN five_instructors fi
        ON lw.instructor_id = fi.instructor_id
        )

        SELECT instructor_id , lesson_start_at 
        FROM classify_lessons 
        WHERE course_rank = 50 
        """
        df = conn.querying(QUERY_1, None)
        print(df)
        df.to_csv(self.result_dir + "result_1.csv", index=False)

    def move_csv_file(self, dir_src, dir_dest, file_name):
        try:
            files = os.listdir(dir_src)
            files = [file for file in files if os.path.isfile(os.path.join(dir_src, file)) and file.endswith(".csv")]
            src_file = os.path.join(dir_src, files[0])
            dest_file = os.path.join(dir_dest, file_name)
            shutil.move(src_file, dest_file)
            shutil.rmtree(dir_src)
            return 1
        except Exception as e:
            print(e)
            return 0

    def custom_function(self, lat_name, lon_name):
        req = Req()
        return req.get_department(lat_name, lon_name)

    def get_partnerships(self):
        conn = ConnectionBigQuery()
        partnerships = []
        if os.path.exists(self.partnership_path):
            df = pd.read_csv(self.partnership_path)    
            partnerships = list(df['partnership_type'])
        
        else:
            QUERY = """
            SELECT DISTINCT partnership_type 
            FROM `test_dataset.instructors`
            """
            df = conn.querying(QUERY, None)
            df.to_csv(self.partnership_path, index=False)
            partnerships = list(df['partnership_type'])
        return partnerships

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
            return False

    def sub_lists(self, list1, list2):
        return [l for l in list1 if l in list2]

    def process_departments(self, df, nb_partitions):
        spark = SparkSession.builder.appName("response 2").getOrCreate()
        spark_df = spark.createDataFrame(df)
        spark_df = spark_df.repartition(nb_partitions)
        upperCaseUDF = udf(lambda x, y: self.custom_function(x, y), StringType())
        spark_df = spark_df.withColumn("departments", upperCaseUDF("mp_latitude", "mp_longitude"))
        spark_df = spark_df.repartition("departments")
        spark_df = spark_df.select("departments")
        spark_df.cache()
        dep_lessons = spark_df.groupBy("departments").count().withColumnRenamed("count", "number_lessons")
        dep_lessons.show(n=dep_lessons.count())
        dep_lessons = dep_lessons.coalesce(1)
        print("end time " + str(datetime.now() - self.current_time))
        return dep_lessons

    def count_department_by_partnership(self, partnership_types: list[str], start_date: str, end_date: str):
        conn = ConnectionBigQuery()
        bq = conn.getBigQuery()
        partners = self.get_partnerships()
        partnership_types = self.sub_lists(partnership_types, partners)
        check_date = self.checking_date(start_date, end_date)
        if check_date == False:
            print("veuillez changer la date de début et celle de fin , car celle de fin ne peût être plus petite que celle du début")
        if len(self.sub_lists(partnership_types, partners)) == 0:
            print("veuiller vérifier les partenaires")
        if check_date == True and len(self.sub_lists(partnership_types, partners)) > 0:
            QUERY_2 = """
            WITH lesson_booked AS (
            SELECT l.lesson_id 
            FROM `test_dataset.lessons` as l
            INNER JOIN `test_dataset.bookings` as b
            USING (lesson_id)
            WHERE b.booking_created_at IS NOT NULL
            AND b.booking_deleted_at IS NULL
            ),

            lesson_with_instructors AS ( 
            SELECT l.lesson_id,l.instructor_id,l.meeting_point_id
            FROM  `test_dataset.lessons` as l
            INNER JOIN lesson_booked as lb
            ON l.lesson_id = lb.lesson_id
            WHERE l.lesson_created_at IS NOT NULL
            AND l.lesson_deleted_at IS NULL
            AND l.lesson_start_at BETWEEN @start_date AND @end_date
            ),


            partnershipping AS (
            SELECT l.lesson_id,l.meeting_point_id
            FROM lesson_with_instructors  as l
            INNER JOIN `test_dataset.instructors` as i 
            USING (instructor_id)
            WHERE partnership_type in UNNEST(@partners)
            )

            SELECT m.mp_latitude , m.mp_longitude ,p.lesson_id
            FROM partnershipping as p
            INNER JOIN `test_dataset.meeting_points` as m
            USING (meeting_point_id)
            """

            job_config = bq.QueryJobConfig(
                query_parameters=[
                    bq.ScalarQueryParameter("start_date", "STRING", start_date),
                    bq.ScalarQueryParameter("end_date", "STRING", end_date),
                    bq.ArrayQueryParameter("partners", "STRING", partnership_types),
                ]
            )

            pandas_df = conn.querying(QUERY_2, job_config)
            nb_deps = pandas_df.shape[0]
            partitions = (pandas_df.shape[0] // 100) + 1
            if pandas_df.shape[0] > 1:
                spark_df = self.process_departments(pandas_df, partitions)
                if spark_df is not None:
                    print("Le nombre de leçons concerné est : " + str(nb_deps))
                    spark_df.write.csv(self.spark_write_dir, mode="overwrite", header=True)
            else:
                spark_df = pandas_df
                print("aucun départements en fonction de ces paramètres")

            return pandas_df.shape[0]


    def launch(self):    
        choice = input(" veuillez rentrer un chiffre entre 1 ou 2 : ")
        if str(choice).isdigit():
            if int(choice) == 1:
                self.get_date_by_instructors()
                print("l'ensemble du résultat est disponible dans le fichier situé au chemin : data/results/result_1.csv")

            if int(choice) == 2:
                all_partners = self.get_partnerships()
                partners = ["SARL", "SAS", "ME"]
                begin_date = '2020-05-01 00:00:00'
                end_date = '2020-05-05 00:00:00'
                print("Veuillez choisir un type de partenariat parmi les suivant : EI,ME,SAS,EIRL,EURL,SARL,SASU")
                partnership_types = input("si plusieurs séparer par un espace : ") 
                partnership_types = partnership_types.split(" ")
                start_date = input("Veuiller entrer la date début au format 'Y-M-D H:M:S' : ")
                finish_date = input("Veuiller entrer la date de fin au format 'Y-M-D H:M:S' : ")
                check_date = self.checking_date(start_date,finish_date)
                if check_date == True and len(self.sub_lists(partnership_types, all_partners)) > 0:
                    result_count = self.count_department_by_partnership(partnership_types, start_date,finish_date)
                    print("Vous avez bien rempli les champs , le script va bien s'exécuter")
                if check_date == False or len(self.sub_lists(partnership_types, all_partners)) == 0:
                    print("Vous n'avez pas bien rempli les champs le script par défaut va s'exécuter")
                    result_count = self.count_department_by_partnership(partners, begin_date,end_date)
                if os.path.exists(self.spark_write_dir) and result_count > 0:
                    self.move_csv_file(self.spark_write_dir, self.result_dir, "result_2.csv")
                    print("l'ensemble du résultat est disponible dans le fichier situé au chemin : data/results/result_2.csv")
            if int(choice) !=1 and int(choice) !=2:
                print("veuiller bien rentrer un chiffre qui est soit 1 ou 2")
        else:
                print("veuiller bien rentrer un chiffre qui est soit 1 ou 2")



    
    