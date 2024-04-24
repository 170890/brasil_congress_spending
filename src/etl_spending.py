from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower
from pyspark.sql.types import IntegerType, LongType
from delta import DeltaTable
from pyspark.sql.types import StructType
import logging

from helper.spark_session_builder import SparkSessionBuilder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def read_csv(name: str, path:str, spark: SparkSession) -> DataFrame:
    
    logger.info(f'Starting read {name}.csv')
    df = spark.read.format("csv").option("delimiter",";").option("inferSchema","True").option("header","True").load(f"{path}.csv")
    return df

def getParties(data: DataFrame) -> DataFrame:
    
    df_parties = data.dropDuplicates(["party_acronym"]).select("party_acronym")
    df_parties = df_parties.dropna()
    return df_parties

def getVendor(data: DataFrame) -> DataFrame:

    df_vendors = data.dropDuplicates(["vendor_id"]).select("vendor_name","vendor_id")
    df_vendors = df_vendors.dropna()
    return df_vendors

def transformSpending(data: DataFrame) -> DataFrame:

    try:
        schema_fields = {
            "txNomeParlamentar": "parliamentary_name",
            "cpf": "cpf",
            "ideCadastro": "registry_id",
            "nuCarteiraParlamentar": "number_parliamentary_wallet",
            "nuLegislatura": "number_legislature",
            "sgUF": "uf",
            "sgPartido": "party_acronym",
            "codLegislatura": "legislature_code",
            "numSubCota": "sub_quota_number",
            "txtDescricao": "description",
            "numEspecificacaoSubCota": "sub_quota_specification_number",
            "txtDescricaoEspecificacao": "specification_description",
            "txtFornecedor": "vendor_name",
            "txtCNPJCPF": "vendor_id",
            "txtNumero": "number",
            "indTipoDocumento": "identifier_document_type",
            "datEmissao": "emission_date","vlrDocumento": "document_value",
            "vlrGlosa": "gloss_value","vlrLiquido": "liquid_value",
            "numMes": "month_number",
            "numAno": "year_number",
            "numParcela": "installment_number",
            "txtPassageiro": "passenger",
            "txtTrecho": "excerpts",
            "numLote": "batch_number",
            "numRessarcimento": "refund_number",
            "vlrRestituicao": "restituition_value",
            "nuDeputadoId": "congress_person_id",
            "ideDocumento": "document_id",
            "urlDocumento": "document_url"
        }

        #old_columns = data.columns
        #new_columns = ["parliamentary_name","cpf","registry_id","number_parliamentary_wallet","number_legislature","uf","party_acronym","legislature_code","sub_quota_number","description","sub_quota_specification_number","specification_description","vendor_name","vendor_id","number","identifier_document_type","emission_date","document_value","gloss_value","liquid_value","month_number","year_number","installment_number","passenger","excerpts","batch_number","refund_number","restituition_value","congressperson_id","document_id","document_url"]

        for old_name, new_name in schema_fields.items():
            data = data.withColumnRenamed(old_name, new_name)
        
        data = data.dropDuplicates(['registry_id'])

        return data
    except Exception as e:
        logger.error("Error occurred during renaming:", e)

def transformVendor(data: DataFrame) -> DataFrame:

    df_vendor = getVendor(data)
    return df_vendor

def transformParty(data: DataFrame) -> DataFrame:

    df_party = getParties(data)
    return df_party

def persistVendor(df_vendor: DataFrame, spark: SparkSession):
    
    logger.info('Persisting vendor')
    vendor_pk = ['vendor_id']
    (DeltaTable.forName(spark, "vendor").merge(df_vendor.alias('df'), 
                    ' and '.join([f'vendor.{k} = df.{k}' for k in vendor_pk]))
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute())

def persistParty(df_party: DataFrame, spark: SparkSession):

    logger.info('Persisting party')
    party_pk = ['party_acronym']
    (DeltaTable.forName(spark, "party").merge(df_party.alias('df'), 
                    ' and '.join([f'party.{k} = df.{k}' for k in party_pk]))
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute())
    
def persistSpending(df_spending: DataFrame, spark: SparkSession):

    logger.info('Persisting spending')
    spending_pk = ['registry_id']
    (DeltaTable.forName(spark, "spending").merge(df_spending.alias('df'), 
                    ' and '.join([f'spending.{k} = df.{k}' for k in spending_pk]))
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute())

    

def process(path: str):
    
    spark = SparkSessionBuilder().build()

    # extract
    df_spending = transformSpending(read_csv('spending',path,spark))

    # transform
    df_vendor = transformVendor(df_spending)
    df_party = transformParty(df_spending)

    # persist
    persistSpending(df_spending, spark)
    persistVendor(df_vendor, spark)
    persistParty(df_party, spark)

    logger.info('Process finished')

if __name__ == '__main__':

    process('resources/data/Ano-2019')
