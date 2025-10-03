import os, dotenv

# CARGAR VARIABLES GLOBALES
dotenv.load_dotenv()
user = os.getenv("USER")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")
user_db = os.getenv("USER_DB")
password_db = os.getenv("PASSWORD_DB")
host_db_pro = os.getenv("HOST_DB_PRO")
host_db_dev = os.getenv("HOST_DB_DEV")
database_dev = os.getenv("DATABASE_DEV")
num_establecimiento_ruc_venture = os.getenv("NUM_ESTABLECIMIENTO_RUC_VENTURE")

class BaseConfig:
    # SECRET_KEY = "key"
    DEBUG = True
    TESTING = True
    HOST = "0.0.0.0"
    PORT = "5000"
    SERVER_NAME = HOST + ":" + PORT
    SQLALCHEMY_DATABASE_URI = "oracle://{0}:{1}@{2}:{3}/{4}".format(
        user, password, host, port, database
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    USER_DB = user_db
    PASSWORD_DB = password_db
    HOST_DB_PRO = host_db_pro
    HOST_DB_DEV = host_db_dev
    DATABASE = database
    PORT_DB = port
    DATABASE_DEV = database_dev
    NUM_ESTABLECIMIENTO_RUC_VENTURE = num_establecimiento_ruc_venture