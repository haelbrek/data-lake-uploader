import os
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Variables d'environnement
KEYVAULT_URL = os.getenv("KEYVAULT_URL")  # URL de votre Key Vault
SECRET_NAME = os.getenv("SECRET_NAME")  # Nom du secret du Service Principal principal
SP_ID_SECONDARY = os.getenv("SP_ID_SECONDARY")  # Service Principal secondaire ID
SP_SECONDARY_PASSWORD = os.getenv("SP_SECONDARY_PASSWORD")  # Mot de passe du Service Principal secondaire
TENANT_ID = os.getenv("TENANT_ID")  # ID de votre locataire Azure
SP_ID_PRINCIPAL = os.getenv("SP_ID_PRINCIPAL")  # Service Principal principal ID
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")  # Nom de votre compte de stockage
FILE_PATH = os.getenv("FILE_PATH")  # Chemin vers votre fichier CSV local
CONTAINER_NAME = os.getenv("CONTAINER_NAME")  # Nom du conteneur dans votre Data Lake
DESTINATION_PATH = os.getenv("DESTINATION_PATH")  # Chemin où le fichier sera téléchargé dans Data Lake

# 1. Authentification avec le Service Principal secondaire pour récupérer le secret du Service Principal principal
credential_secondary = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=SP_ID_SECONDARY,
    client_secret=SP_SECONDARY_PASSWORD
)
print(credential_secondary)

# Créer le client du Key Vault pour accéder aux secrets
secret_client = SecretClient(vault_url=KEYVAULT_URL, credential=credential_secondary)

# Récupérer le secret du Service Principal principal depuis le Key Vault
secret = secret_client.get_secret(SECRET_NAME)
sp_secret = secret.value
print(f"Secret du Service Principal principal récupéré depuis le Key Vault.")

# 2. Authentification avec le Service Principal principal en utilisant le secret récupéré
credential_principal = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=SP_ID_PRINCIPAL,
    client_secret=sp_secret
)

# Connexion au service Azure Blob Storage (Data Lake)
STORAGE_URL = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
blob_service_client = BlobServiceClient(account_url=STORAGE_URL, credential=credential_principal)

# 3. Fonction pour télécharger un fichier dans Data Lake
def upload_file_to_data_lake(file_path, container_name, destination_path):
    try:
        # Accéder au conteneur du Data Lake
        container_client = blob_service_client.get_container_client(container_name)

        # Ouvrir le fichier local et le télécharger
        with open(file_path, "rb") as file_data:
            container_client.upload_blob(
                name=destination_path,
                data=file_data,
                overwrite=True  # Permet d'écraser si le fichier existe déjà
            )
        print(f"Fichier {file_path} téléchargé avec succès dans {destination_path} du Data Lake.")
    except Exception as e:
        print(f"Erreur lors du téléchargement du fichier : {e}")

# 4. Appeler la fonction pour charger le fichier CSV dans le Data Lake
upload_file_to_data_lake(FILE_PATH, CONTAINER_NAME, DESTINATION_PATH)

