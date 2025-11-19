import os
import csv
import re
import multiprocessing as mp
import time
from datetime import datetime
import mysql.connector
from mysql.connector import Error

total_processors = os.cpu_count()
folder_path = './samples/EdNet-KT2-samples'

#folder_path = '/home/guipeeix/Downloads/EdNet-KT2/KT2'
counterFiles = 0

DB_CONFIG = {
    'user': 'user',
    'password': 'mysqlPass',
    'host': 'tcc-cola',
    'database': 'tccdb'
}

def unix_to_mysql_datetime(timestamp):
    ts = float(timestamp) / 1000  # Convert to float first, then divide
    dt = datetime.fromtimestamp(ts)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def truncate_respostas():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("TRUNCATE TABLE respostas_lake")
        conn.commit()  
        
        print("Tabela 'respostas_lake' foi limpa.")
        return True
    
    except Error as err:
        if conn:
            conn.rollback()
        print(f"Erro durante truncagem: {err}")
        return False
    
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def create_lake_source():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("INSERT INTO lake_source (name, created_at, updated_at) VALUES ('kt-migration', NOW(), NOW());")
        conn.commit()  
        
        print("Tabela 'respostas_lake' foi limpa.")
        return True
    
    except Error as err:
        if conn:
            conn.rollback()
        print(f"Erro durante truncagem: {err}")
        return False
    
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def insert_data(record_array, conn):
    cursor = None
    conn = mysql.connector.connect(**DB_CONFIG)

    try:
        cursor = conn.cursor()
        
        insert_sql = """
            INSERT INTO respostas_lake (item_id, resposta_usuario, respondida_em, user_id, source_id)
            VALUES (%s, %s, %s, %s, 1)
        """
        
        cursor.executemany(insert_sql, record_array)
        
        conn.commit()
        
        return len(record_array)
        
        
    except Error as err:
        if conn:
            conn.rollback()
        print(f"Database error: {err}")
        return 0
        
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

def processar_e_salvar(csv_files):
    result = {
        "name": "EdNet-KT2-samples",
        "exams": []
    }
    

    conn = None    
    for filename in csv_files:
        user_id_match = re.search(r'u(\d+)\.csv', filename)
        if not user_id_match:
            continue

        user_id = int(user_id_match.group(1))
        file_path = os.path.join(folder_path, filename)
        
        user_exam = []

        
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)            

            for row in reader:
                if row['action_type'] == 'respond' and row['item_id'].startswith('q'):
                    question_id = int(row['item_id'][1:])
                    #row['timestamp']
                    exam_entry = (
                        question_id,
                        row['user_answer'] if row['user_answer'] else None,
                        unix_to_mysql_datetime(row['timestamp'] ) if row['timestamp'] else None,
                        user_id,
                    )
                    
                    user_exam.append(exam_entry)

            insert_data(user_exam, conn)
        
    return result

def process_csv_files(folder_path):
    
    
    if not os.path.exists(folder_path):
        print(f"Folder {folder_path} not found")
        return None
    
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    

    totalFiles = 0

    totalFiles += sum(1 for row in csv_files)


    tamanho_grupo = totalFiles // total_processors; 
    if(tamanho_grupo == 0 ):
        tamanho_grupo = 1
    
    grupos = [
        csv_files[i:i + tamanho_grupo] 
        for i in range(0, len(csv_files), tamanho_grupo)
    ]
    

    inicio = time.time()
    with mp.Pool(processes=total_processors) as pool:
        resultados = pool.map(processar_e_salvar, grupos)    

def wait_for_db(max_retries=30, delay=2):
    for attempt in range(max_retries):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            if conn.is_connected():
                conn.close()
                return True
        except Error as err:
            print("retrying")
            time.sleep(delay)
    print("No connection :<")
    return False

#In the future i gonna use the json to send data to backend, as json mainly. 

#Importante
#Criar a base de dados mysql sem o innoDb para acelearar a consulta e se livrando de FK OK
#Modelar a tabela  OK
#Como vou modelar a tabela, sendo que tenho dois bancos de dados completamente diferentes ??????? Utilizo o conceito de meta_keys e meta_values ?  OK
#Iniciar processo de paralelização, dividindo o número de colunas por workers que vão trabalhar paralelamente para as duas tabelas OK
#inserir no banco de dados a partir disso OK
def main():
    print('init')
    if not wait_for_db():
        exit(1)
    
    create_lake_source()

    truncate_respostas()
    data = process_csv_files(folder_path)

    print("data successfully migrated <:")

if __name__ == "__main__":
    main()