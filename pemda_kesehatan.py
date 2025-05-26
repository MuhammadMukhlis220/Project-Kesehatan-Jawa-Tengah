from airflow import DAG
from datetime import datetime, date
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from sqlalchemy import create_engine
import psycopg2
from opensearchpy.helpers import bulk
from opensearchpy import OpenSearch
import pandas as pd
import uuid
from hdfs import InsecureClient
import os
import pyarrow as pa
import pyarrow.orc as orc
import io

hdfs_node = Variable.get("hdfs_poc")
hdfs_client = InsecureClient(f'http://{hdfs_node}:50070', user='airflow')

dir_hdfs = "/hive_external_data/ds_pemda/kesehatan/jawa_tengah"
folder = [
          "faskes", 
          "nakes", 
          "penduduk_miskin", 
          "jumlah_penyakit_berdasarkan_jenis", 
          "jumlah_bayi_bblr_gizi_buruk",
          "lokasi"
        ]

table_postgre = [
          "faskes_jateng", 
          "nakes_jateng", 
          "penduduk_miskin_jateng", 
          "jumlah_penyakit_berdasarkan_jenis_jateng", 
          "jumlah_bayi_bblr_gizi_buruk_jateng",
          "lokasi_jawa_tengah"
        ]

def postgre_to_hive():

    engine = create_engine('postgresql+psycopg2://x:x@x.x.x.x/ds_prod')
    
    for folder_name, table_name in zip(folder, table_postgre):
        dir_hdfs_folder = f"{dir_hdfs}/{folder_name}"
        
        # HDFS FOLDER
        if hdfs_client.status(dir_hdfs_folder, strict=False) is not None:
            hdfs_client.delete(dir_hdfs_folder, recursive=True)
            print(f"Hapus folder: {dir_hdfs_folder}")

        hdfs_client.makedirs(dir_hdfs_folder)
        print(f"Buat folder: {dir_hdfs_folder}")

        os.system(f'hdfs dfs -chmod 777 {dir_hdfs_folder}')
        print(f"Set permissions to 777 for {dir_hdfs_folder}")

        # AMIBL DATA
        with engine.connect() as conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, con=conn)

        df['execute_time'] = date.today()
        df.columns = [col.strip().lower().replace(" ", "_").replace(".","") for col in df.columns]
        
        # ORC
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        with orc.ORCWriter(buffer) as writer:
            writer.write(table)

        orc_data = buffer.getvalue()

        # HDFS
        orc_path = f"{dir_hdfs_folder}/{folder_name}.orc"
        with hdfs_client.write(orc_path, overwrite=True) as writer:
            writer.write(orc_data)

        print(f"✅ Berhasil write ORC ke HDFS: {orc_path}")
    

def postgre_to_opensearch():
    
    engine = create_engine('postgresql+psycopg2://x:x@x.x.x.x/ds_prod')
    conn = None

    try:
        conn = engine.raw_connection()
        conn.autocommit = True
        
    # DI BAWAH INI ADALAH UNTUK NUSA TENGGARA TIMUR
        '''
        query = """
        SELECT 
          n."Wilayah", 
          n."Tahun", 
          n."Tenaga Kesehatan - Perawat", 
          n."Tenaga Kesehatan - Bidan", 
          n."Tenaga Kesehatan - Tenaga Kefarmasian", 
          n."Tenaga Kesehatan - Tenaga Kesehatan Masyarakat", 
          n."Tenaga Kesehatan - Tenaga Kesehatan Lingkungan", 
          n."Tenaga Kesehatan - Tenaga Gizi", 
          n."Jumlah Tenaga Medis", 
          n."Jumlah Tenaga Kesehatan Psikologi Klinis", 
          n."Jumlah Tenaga Keterapian Fisik", 
          n."Jumlah Tenaga Keteknisan Medis", 
          n."Jumlah Tenaga Teknik Biomedika", 
          n."Jumlah Tenaga Kesehatan Tradisional", 
          s."Jumlah Balita",
          s."normal" "Tidak Stunting",
          s."stunting" "Jumlah Stunting",
          s."persentase" "Persentase Stunting",
          n."Lat", 
          n."Long"
        FROM public.nakes_ntt n
        LEFT JOIN public.stunting_ntt s
          ON n."Wilayah" = s."wilayah" and n."Tahun" = s."tahun"
        """
        df = pd.read_sql(query, con=conn)
        
        #df.rename(columns={"tahun": "Tahun"}, inplace = True)
        
        df["Location"] = df.apply(lambda row: {"lat": row["Lat"], "lon": row["Long"]}, axis=1)
        df.drop(columns=["Lat", "Long"], inplace=True)
        
        df["Jumlah Nakes"] = ( # UNTUK TABEL nakes_ntt
            df["Tenaga Kesehatan - Perawat"].fillna(0) +
            df["Tenaga Kesehatan - Bidan"].fillna(0) +
            df["Tenaga Kesehatan - Tenaga Kefarmasian"].fillna(0) +
            df["Tenaga Kesehatan - Tenaga Kesehatan Masyarakat"].fillna(0) +
            df["Tenaga Kesehatan - Tenaga Kesehatan Lingkungan"].fillna(0) +
            df["Tenaga Kesehatan - Tenaga Gizi"].fillna(0) +
            df["Jumlah Tenaga Medis"].fillna(0) +
            df["Jumlah Tenaga Kesehatan Psikologi Klinis"].fillna(0) +
            df["Jumlah Tenaga Keterapian Fisik"].fillna(0) +
            df["Jumlah Tenaga Keteknisan Medis"].fillna(0) +
            df["Jumlah Tenaga Teknik Biomedika"].fillna(0) +
            df["Jumlah Tenaga Kesehatan Tradisional"].fillna(0)
        )
        
        '''
    
    # DATA JAWA TENGAH
        #DEFAULT QUERY
        '''
        SELECT a.*, b."Lat", b."Long" FROM penduduk_miskin_jateng a
        LEFT JOIN lokasi_jawa_tengah b on a."Wilayah" = b."Wilayah"
        '''
    
        query = '''
        SELECT 
            a."Wilayah", 
            "Tahun", 
            "Angka Penemuan TBC per 100.000 penduduk" AS "Angka Penemuan TBC per 100 Ribu Penduduk",
            "Jumlah Kasus Baru AIDS" AS "Jumlah Kasus Baru AIDS",
            "Penemuan Kasus Baru Kusta per 100.000 penduduk" AS "Penemuan Kasus Baru Kusta per 100 Ribu Penduduk",
            "Angka Kesakitan Malaria per 1.000 penduduk" AS "Angka Kesakitan Malaria per 1000 Penduduk",
            "Angka Kesakitan DBD per 100.000 penduduk" AS "Angka Kesakitan DBD per 100 Ribu Penduduk",
            b."Lat", 
            b."Long"
        FROM 
            public.jumlah_penyakit_berdasarkan_jenis_jateng a
        LEFT JOIN 
            public.lokasi_jawa_tengah b
            ON a."Wilayah" = b."Wilayah";

        '''
        df = pd.read_sql(query, con=conn)
    
        df["Location"] = df.apply(lambda row: {"lat": row["Lat"], "lon": row["Long"]}, axis=1)
        df.drop(columns=["Lat", "Long"], inplace=True)    
        
        # MODIFY FASKES
        '''
        df["Jumlah Rumah Sakit"] = df["Rumah Sakit Umum"] + df["Rumah Sakit Khusus"]
        df["Puskesmas"] = df["Puskesmas Non Rawat Inap"] + df["Puskesmas Rawat Inap"]
        df["Jumlah Fasilitas Kesehatan"] = df["Rumah Sakit Umum"] + df["Rumah Sakit Khusus"] + df["Puskesmas Non Rawat Inap"] + df["Puskesmas Rawat Inap"] + df["Klinik Pratama"]
        '''
        
        # MODIFY NAKES
        
        # MODIFY penduduk_miskin_jateng
        '''
        SELECT 
            a.*,
            ROUND(
                CAST(("Jumlah Penduduk Miskin Maret Ribu Jiwa" / "Jumlah Penduduk Ribu Jiwa") * 100000 AS numeric), 
                2
            ) AS "Rasio Miskin per 100 Ribu Jiwa",
            b."Lat", b."Long"
        FROM 
            public.penduduk_miskin_jateng a
        left join public.lokasi_jawa_tengah b
        on a."Wilayah" = b."Wilayah";
        '''
        
        # MODIFY BAYI BBLR GIZI BURUK
        '''
        SELECT 
            a.*,
            ROUND(
                CAST( 
                    (a."Jumlah Bayi BBLR" * 1000.0) / NULLIF(a."Jumlah Bayi Lahir", 0) 
                    AS NUMERIC
                ), 
                2
            ) AS "BBLR per 1000 Bayi Lahir",
            ROUND(
                CAST( 
                    (a."Jumlah Kurang Gizi" * 1000.0) / NULLIF(a."Jumlah Bayi Lahir", 0) 
                    AS NUMERIC
                ), 
                2
            ) AS "Bayi Kurang Gizi per 1000",
            b."Lat", 
            b."Long"
        FROM 
            public.jumlah_bayi_bblr_gizi_buruk_jateng a
        LEFT JOIN 
            public.lokasi_jawa_tengah b
            ON a."Wilayah" = b."Wilayah";

        '''
        
        # MODIFY SEBARAN JENIS PENYAKIT
        '''
        SELECT 
            a."Wilayah", 
            "Tahun", 
            "Angka Penemuan TBC per 100.000 penduduk" AS "Angka Penemuan TBC per 100 Ribu Penduduk",
            "Jumlah Kasus Baru AIDS" AS "Jumlah Kasus Baru AIDS",
            "Penemuan Kasus Baru Kusta per 100.000 penduduk" AS "Penemuan Kasus Baru Kusta per 100 Ribu Penduduk",
            "Angka Kesakitan Malaria per 1.000 penduduk" AS "Angka Kesakitan Malaria per 1000 Penduduk",
            "Angka Kesakitan DBD per 100.000 penduduk" AS "Angka Kesakitan DBD per 100 Ribu Penduduk",
            b."Lat", 
            b."Long"
        FROM 
            public.jumlah_penyakit_berdasarkan_jenis_jateng a
        LEFT JOIN 
            public.lokasi_jawa_tengah b
            ON a."Wilayah" = b."Wilayah";
        
        '''
        
        df['execute_time'] = date.today()

    finally:
        if conn is not None:
            conn.close()
            
    OPENSEARCH_HOST = "x.x.x.x"
    OPENSEARCH_PORT = "9220"
    OPENSEARCH_INDEX = "jateng_penyakit_berdasarkan_jenis"
    OPENSEARCH_TYPE = "_doc"
    OPENSEARCH_URL = "https://x:x@x.x.x.x:9220/"
    OPENSEARCH_CLUSTER = "ONYX-analytic"
    ONYX_OS = OpenSearch(
                         hosts = [{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}], http_auth = ("admin", "admin"),
                         use_ssl = True, verify_certs = False, ssl_assert_hostname = False, ssl_show_warn = False
                        )
    batch = 100000
    

    for x in range(0, len(df), batch):
        df = df.iloc[x:x+batch]
        hits = [{"_op_type": "index", "_index": OPENSEARCH_INDEX, "_id": str(uuid.uuid4()), "_score": 1, "_source": i} for i in df.to_dict("records")]
        resp, err = bulk(ONYX_OS, hits, index=OPENSEARCH_INDEX, max_retries=3)
        print(resp, err)

with DAG(
    'ds_pemda_kesehatan',
    default_args={
        'start_date': datetime(2025, 5, 1)
    },
    schedule_interval=None,
    catchup=False,
    tags=['dev', 'kesehatan']
) as dag:
    
    t1 = PythonOperator(
        task_id='postgre_to_hive',
        python_callable=postgre_to_hive,
    )
    
    t2 = PythonOperator(
        task_id='postgre_to_opensearch',
        python_callable=postgre_to_opensearch,
    )
    
    
t1 >> t2
