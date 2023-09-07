import pyodbc
import io
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

class ItemDataBase:
    def __init__(self) -> None:
        self.cnx = pyodbc.connect('DRIVER={SQL Server};SERVER=LOCALHERNAN;DATABASE=local')
        self.cursor = self.cnx.cursor()

    def add_item(self,id,body):
        qry = "insert into hr_deparment(id_deparment, name_deparmet) values ('{id}','{body['name']}')"

    def create_backup(self,file):
        with io.open(file, 'w') as f:
            for d in self.interdump():
                f.write('%s\n' % d)
    avro_writer = AvroHelper('back_up1.avsc', file)

db = ItemDataBase()
db.cursor.executemany(qry,get_list)
db.create_backup()


#cursor.execute("select id_deparment, name_deparmet from hr_deparment")
#row = cursor.fetchone()
#if row:
 #   print(row)