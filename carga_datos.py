from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType, ArrayType, FloatType
from pyspark.sql.functions import col, sum, first, udf, expr, mean, max, min, stddev


def iniciaSpark(debug_mode=False):
    spark = SparkSession.builder \
            .appName("Proyecto - Videojuegos ESRB") \
            .config("spark.driver.extraClassPath", "postgresql-42.2.14.jar") \
            .config("spark.executor.extraClassPath", "postgresql-42.2.14.jar") \
            .getOrCreate()
    if not debug_mode:
        spark.sparkContext.setLogLevel('ERROR')
        # print('VERSION: ', spark.sparkContext.version)
    return spark


def load_sales_data(spark_sesion, sales_csv):
    sales_df = spark_sesion \
    .read \
    .format("csv") \
    .option("path", sales_csv) \
    .option("header", True) \
    .schema(StructType([
                StructField("Rank", IntegerType()),
                StructField("Name", StringType()),
                StructField("basename", StringType()),
                StructField("Genre", StringType()),
                StructField("ESRB_Rating", StringType()),
                StructField("Platform", StringType()),
                StructField("Publisher", StringType()),
                StructField("Developer", StringType()),
                StructField("VGChartz_Score", FloatType()),
                StructField("Critic_Score", FloatType()),
                StructField("User_Score", FloatType()),
                StructField("Total_Shipped", FloatType()),
                StructField("Global_Sales", FloatType()),
                StructField("NA_Sales", FloatType()),
                StructField("PAL_Sales", FloatType()),
                StructField("JP_Sales", FloatType()),
                StructField("Other_Sales", FloatType()),
                StructField("Year", FloatType()),
                StructField("Last_Update", StringType()),
                StructField("url", StringType()),
                StructField("status", IntegerType()),
                StructField("Vgchartzscore", FloatType()),
                StructField("img_url", StringType())
               ])) \
    .load()
    return sales_df

def load_ratings_data(spark_sesion, ratings_csv):
    ratings_df = spark_sesion \
    .read \
    .format("csv") \
    .option("path", ratings_csv) \
    .option("header", True) \
    .schema(StructType([
                StructField("title", StringType()),
                StructField("console", IntegerType()),
                StructField("alcohol_reference", IntegerType()),
                StructField("animated_blood", IntegerType()),
                StructField("blood", IntegerType()),
                StructField("blood_and_gore", IntegerType()),
                StructField("cartoon_violence", IntegerType()),
                StructField("crude_humor", IntegerType()),
                StructField("drug_reference", IntegerType()),
                StructField("fantasy_violence", IntegerType()),
                StructField("intense_violence", IntegerType()),
                StructField("language", IntegerType()),
                StructField("lyrics", IntegerType()),
                StructField("mature_humor", IntegerType()),
                StructField("mild_blood", IntegerType()),
                StructField("mild_cartoon_violence", IntegerType()),
                StructField("mild_fantasy_violence", IntegerType()),
                StructField("mild_language", IntegerType()),
                StructField("mild_lyrics", IntegerType()),
                StructField("mild_suggestive_themes", IntegerType()),
                StructField("mild_violence", IntegerType()),
                StructField("no_descriptors", IntegerType()),
                StructField("nudity", IntegerType()),
                StructField("partial_nudity", IntegerType()),
                StructField("sexual_content", IntegerType()),
                StructField("sexual_themes", IntegerType()),
                StructField("simulated_gambling", IntegerType()),
                StructField("strong_janguage", IntegerType()),
                StructField("strong_sexual_content", IntegerType()),
                StructField("suggestive_themes", IntegerType()),
                StructField("use_of_alcohol", IntegerType()),
                StructField("use_of_drugs_and_alcohol", IntegerType()),
                StructField("violence", IntegerType()),
                StructField("esrb_rating", StringType())
               ])) \
    .load()
    return ratings_df

def drop_cols(sales_df):
    """[summary]

    Args:
        sales_df ([type]): [description]
    """
    # Se eliminan las columnas con muchos valores nulos y las que no son reelevantes para el estudio
    df = sales_df.drop('VGChartz_Score', 'Critic_Score', 'User_Score', 
                        'ESRB_Rating', 'Vgchartzscore', 'Last_Update', 'url',
                        'status', 'img_url', 'Total_Shipped')
    return df


def cast_year_col(sales_df):
    # Se castea la columna Year a tipo entero
    df = sales_df.withColumnRenamed('Year', 'Year_float')
    df = df.withColumn('Year', df['Year_float'].cast(IntegerType()))
    df = df.drop('Year_float')
    return df

def drop_global_sales_nulls(sales_df):
    # Dropea las filas con nulos y 0s en global sales
    df = sales_df.na.drop(subset=["Global_Sales"])
    df = df.filter(sales_df.Global_Sales > 0)
    return df

def group_sales_by_game(sales_df):
    # Las ventas del mismo juego se encuentran separadas por plataforma
    # Se calculan las ventas totales de cada juego en total
    grouped_sales_df = sales_df.select('*').groupBy('Name').agg(sum('Global_Sales'), 
                    first('Genre'), first('Platform'), first('Publisher'), 
                    first('Developer'), first('Year'))
    grouped_sales_df = grouped_sales_df.withColumnRenamed('sum(Global_Sales)', 'Global_Sales')
    grouped_sales_df = grouped_sales_df.withColumnRenamed('first(Genre)', 'Genre')
    grouped_sales_df = grouped_sales_df.withColumnRenamed('first(Platform)', 'Platform')
    grouped_sales_df = grouped_sales_df.withColumnRenamed('first(Publisher)', 'Publisher')
    grouped_sales_df = grouped_sales_df.withColumnRenamed('first(Developer)', 'Developer')
    grouped_sales_df = grouped_sales_df.withColumnRenamed('first(Year)', 'Year')
    return grouped_sales_df

def add_target_col(df, col_name):
    percentile_75 = df.agg(expr('percentile({}, array(0.75))'.format(col_name))[0].alias('%75')).collect()[0]
    percentile_75 = percentile_75[0]
    
    successful_UDF = udf(lambda x: (
                    1 if (
                        x >= percentile_75
                    )
                    else 0
        ),
        IntegerType()
    )

    successful_df = df.withColumn('Successful', successful_UDF(df[col_name]))
    return successful_df

def normalize_col(df, col_name):
    max_min_df = df.select(max(col_name).alias("max_" + col_name), min(col_name).alias("min_" + col_name))
    max_gs = max_min_df.collect()[0][0]
    min_gs = max_min_df.collect()[0][1]

    diff = max_gs - min_gs
    df_scaled = df.withColumn(col_name + "_scaled" , (col(col_name) - min_gs) / diff)
    return df_scaled

def join_dfs(df1, df2):
    sales_ratings_df = df1.join(df2, df1.Name == df2.title, how='inner')
    sales_ratings_df = sales_ratings_df.drop('title')
    return sales_ratings_df


def write_df_in_db(df, table_name):
    # Almacenar el conjunto de datos limpio en la base de datos
    df \
    .write \
    .format("jdbc") \
    .mode('overwrite') \
    .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
    .option("user", "postgres") \
    .option("password", "testPassword") \
    .option("dbtable", table_name) \
    .save()
