## **Project-Kesehatan-Jawa-Tengah**
---
Data Source:
1. https://jateng.bps.go.id/id/statistics-table/2/MjEwMiMy/jumlah-fasilitas-kesehatan-menurut-jenis-fasilitas-kesehatan-dan-kabupaten-kota-di-provinsi-jawa-tengah.html
2. https://jateng.bps.go.id/id/statistics-table/1/MjU4MiMx/jumlah-tenaga-kesehatan-menurut-kabupaten-kota-di-provinsi-jawa-tengah--2021.html
3. https://jateng.bps.go.id/id/statistics-table/3/UkVkWGJVZFNWakl6VWxKVFQwWjVWeTlSZDNabVFUMDkjMw==/jumlah-dan-persentase-penduduk-miskin-menurut-kabupaten-kota-di-provinsi-jawa-tengah--2023.html?year=2020
4. https://jateng.bps.go.id/id/statistics-table/2/NDgwIzI=/jumlah-kasus-penyakit-menurut-jenis-penyakit-menurut-kabupaten-kota-di-provinsi-jawa-tengah.html
5. https://jateng.bps.go.id/id/statistics-table/2/Mzc4IzI=/jumlah-bayi-lahir--bayi-berat-badan-lahir-rendah--bblr---dan-bergizi-kurang-menurut-kabupaten-kota-di-provinsi-jawa-tengah--jiwa-.html

**For ez data, go to [data-source](https://github.com/MuhammadMukhlis220/Project-Kesehatan-Jawa-Tengah/tree/main/data-source)**

Work flow:
![Alt Text](/pic/workflow.png)

Figure 1

### PostgreSQL

Get all data from [data-source](https://github.com/MuhammadMukhlis220/Project-Kesehatan-Jawa-Tengah/tree/main/data-source) and export it to postgre. I use import feature from [dbeaver](https://dbeaver.io/) like figure 2 bellow:

![Alt Text](/pic/import_postgresql_1.png)

Figure 2

You can follow the next step by using "upload via csv" to create table. And the result is in figure 3:

![Alt Text](/pic/import_postgresql_2.png)

Figure 3

### Airflow - HDFS & OpenSearch

After data already in postgre, just upload my [code](pemda_kesehatan.py) to your Airflow (**remember to change the variable hdfs_node since i am using variable feature**. Look at DAG airflow named `ds_pemda_kesehatan`. There will be 2 tasks, first one is to HDFS (you can create the table using dbeaver too in hive in [here](/data-source/create_hive_for_pemda_kesehatan.txt). And last one is for OpenSearch's index. **Run the DAG for HDFS's task first. Don't run both of them. It will cause error since the code is created for development by one by one (1 task run 5 times since there are 5 tables). After HDFS's task, run for OpenSearch's task with same pattern.**

### HDFS - HIVE

Result if using [dbeaver](https://dbeaver.io/):

![Alt Text](/pic/import_hive_1.png)

Figure 4

### OpenSearch

There are some index with scipt field to geneate filed in index pattern (Field `Wilayah Laju Penyakit` and field `WIlayah Bayi BBLR`)

For the complete script field code (painless), refer to the following block for field `Wilayah Laju Penyakit`.
<details>
   <summary>Click to view the complete code.</summary>

   ```painless

String wilayah = doc['Wilayah.keyword'].value; List tinggi = [ "Kabupaten Banjarnegara", "Kabupaten Sragen", "Kabupaten Brebes", "Kabupaten Wonosobo", "Kabupaten Demak", "Kabupaten Wonogiri", "Kabupaten Magelang", "Kabupaten Karanganyar", "Kabupaten Kebumen", "Kabupaten Kendal" ]; List rendah = [ "Kabupaten Cilacap", "Kota Pekalongan", "Kota Salatiga", "Kabupaten Blora", "Kabupaten Batang" ]; if (tinggi.contains(wilayah)) { return "Wilayah Laju Penyakit Tinggi"; } else if (rendah.contains(wilayah)) { return "Wilayah Laju Penyakit Rendah"; } else { return "Wilayah Laju Penyakit Sedang"; }

   ```
   </details>

And here for field `Wilayah Bayi BBLR`.
<details>
   <summary>Click to view the complete code.</summary>

   ```painless

String wilayah = doc['Wilayah.keyword'].value; List rendah = [ 'Kota Surakarta', 'Kota Salatiga', 'Kota Semarang', 'Kabupaten Pemalang', 'Kabupaten Wonogiri' ]; if (rendah.contains(wilayah)) { return "Wilayah Bayi BBLR Rendah"; } else { return "Wilayah Bayi BBLR Tinggi"; }

   ```
   </details>

<br>

![Alt Text](/pic/import_opensearch_1.png)

Figure 5

After index pattern created, cook the dashboard. For snapshot:
<br>
![Alt Text](/pic/dashboard.png)

Figure 6

### Zeppelin

It for analytics purpose and for my notes.

<br>
<br>

That all, happy experimenting!
