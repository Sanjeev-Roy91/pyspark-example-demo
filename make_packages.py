import zipfile

with zipfile.ZipFile('packages.zip','w',compression=zipfile.ZIP_DEFLATED) as my_zip:
    my_zip.write('dependencies\logging.py')
    my_zip.write('dependencies\spark_conn.py')

