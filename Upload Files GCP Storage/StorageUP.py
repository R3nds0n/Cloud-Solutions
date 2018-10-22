import os,time,glob
from google.cloud import storage
from google.cloud import language
from tqdm import tqdm

def List_Path(path,cat):
    array_dir_nas = []
    if cat == "dirt":
        for root, dirs, files in os.walk(path):
            if dirs != []:
                for dir in dirs:
                    array_dir_nas.append(dir)
    if cat == "file":
        for root, dirs, files in os.walk(path):
            if files != []:
                for fil in files:
                    array_dir_nas.append(fil)
    return array_dir_nas

# Instanciando o cliente GCP
def Start_Clients():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = dir_api_key
    client = language.LanguageServiceClient()
    storage_client = storage.Client()
    return storage_client

# Listando todos os Buckets
def List_Bucket():
    for bucket in storage_client.list_buckets():
        print(bucket)

# Buscando Bucket Definido
def Get_Bucket(path):
    bucket = storage.Client().get_bucket(path)
    return bucket

# Buscando Arquivos no Bucket
def Get_Blobs():
    bucket = Get_Bucket(dir_bkt_doc)
    prefix = dir_pfx_bkt
    bucket.list_blobs()

    if prefix != "":
        blobs = bucket.list_blobs(prefix=prefix)
    else:
        blobs = bucket.list_blobs()

    return blobs

# Quebrando os diretórios NAS
def Insert_Doc():
    array_docs_gc = []
    for blob in blobs:
        if len(str(blob.name).split('/')[1]) == 8:
            file = str(blob.name).split('/')[0] + '/' + \
                   str(blob.name).split('/')[1] + '/' + \
                   str(blob.name).split('/')[2]
            array_docs_gc.append(file)
    return array_docs_gc

# Atualizar o Google Cloud
def Update_GCP():
    loop = len(array_upd_gc)
    if loop > 0:
        bucket = Get_Bucket(dir_bkt_doc)
        with tqdm(total=loop) as pbar:
            for doc in array_upd_gc:
                blob = bucket.blob(doc)
                blob.upload_from_filename(dir_pth_doc + doc)
                pbar.update(1)
                print

        print('')
        print("     Arquivo(s) atualizado(s) no GCP: " + str(loop))

if __name__ == "__main__":

    # Validação de acesso
    dir_api_key = 'C:/GOOGLE_API/StorageCenturyLink-819868803cfa.json'
    storage_client = Start_Clients()

    # Diretórios de arquivos GCP e NAS
    dir_bkt_doc = 'nas-vhe'
    dir_pth_doc = 'Y:/'

    # Percorrendo a Unidade NAS
    for dir in os.listdir(dir_pth_doc):
        if len(dir) == 3 :
            print('Atualizando .... ' + dir)
            dir_pfx_bkt = dir

            # Criando array de documentos GCP
            blobs = Get_Blobs()
            array_dir_gc = Insert_Doc()
            print("     Arquivo(s) no GCP: " + str(len(array_dir_gc)) )

            # Criando array de documentos NAS
            array_dir_nas = []
            array_pth_nas = List_Path(dir_pth_doc + dir_pfx_bkt,"dirt")
            for dir in array_pth_nas:
                if len(dir) == 8:
                    for file in List_Path(dir_pth_doc + dir_pfx_bkt + '/' + dir, "file"):
                        array_dir_nas.append(dir_pfx_bkt + '/' + dir + '/' + file)
            print("     Arquivo(s) no NAS: " + str(len(array_dir_nas)) )

            # Atualizando o GCP
            array_upd_gc = set(array_dir_nas).difference(set(array_dir_gc))
            Update_GCP()
