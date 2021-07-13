from carga_datos import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, ArrayType
import pytest

@pytest.fixture(scope="module")
def spark_session():
    """A fixture to create a Spark Context to reuse across tests."""
    s = SparkSession.builder.appName('pytest-local-spark').master('local') \
        .getOrCreate()

    yield s

    s.stop()


def test_drop_cols(spark_session):
    sales_schema = StructType([
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
               ])

    data =  [
        (1, 'Metal Gear Solid 2: Sons of Liberty', 'metal-gear-solid-2-sons-of-liberty',	
        'Action', 'M', 'PS2', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 ,6.05,	2.45,	2.01,	0.87,	0.72,	2001.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-2-sons-of-liberty/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg'),
        (2, 'Metal Gear Solid 3: Snake Eater', 'metal-gear-solid-3-snake-eater',	
        'Action', 'M', 'PS2', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 ,6.05,	2.45,	2.01,	0.87,	0.72,	2004.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-3-snake-eater/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg'),
                ]

    df = spark_session.createDataFrame(data, sales_schema)
    expected_df = df.drop('VGChartz_Score', 'Critic_Score', 'User_Score', 
                        'ESRB_Rating', 'Vgchartzscore', 'Last_Update', 'url',
                        'status', 'img_url', 'Total_Shipped')

    actual_df = drop_cols(df)

    assert actual_df.collect() == expected_df.collect(), 'No se generó el dataframe esperado.'

def test_cast_year_col(spark_session):
    sales_schema = StructType([
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
               ])

    data =  [
        (1, 'Metal Gear Solid 2: Sons of Liberty', 'metal-gear-solid-2-sons-of-liberty',	
        'Action', 'M', 'PS2', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 ,6.05,	2.45,	2.01,	0.87,	0.72,	2001.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-2-sons-of-liberty/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg'),
        (2, 'Metal Gear Solid 3: Snake Eater', 'metal-gear-solid-3-snake-eater',	
        'Action', 'M', 'PS2', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 ,6.05,	2.45,	2.01,	0.87,	0.72,	2004.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-3-snake-eater/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg'),
                ]

    df = spark_session.createDataFrame(data, sales_schema)
    expected_df = df.withColumnRenamed('Year', 'Year_float')
    expected_df = expected_df.withColumn('Year', expected_df['Year_float'].cast(IntegerType()))
    expected_df = expected_df.drop('Year_float')
    
    actual_df = cast_year_col(df)

    assert actual_df.collect() == expected_df.collect(), 'No se generó el dataframe esperado.'

def test_drop_global_sales_nulls(spark_session):
    sales_schema = StructType([
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
               ])

    actual_data =  [
        (1, 'Metal Gear Solid 2: Sons of Liberty', 'metal-gear-solid-2-sons-of-liberty',	
        'Action', 'M', 'PS2', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 ,6.05,	2.45,	2.01,	0.87,	0.72,	2001.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-2-sons-of-liberty/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg'),
        (2, 'Metal Gear Solid 3: Snake Eater', 'metal-gear-solid-3-snake-eater',	
        'Action', 'M', 'PS2', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 , None, 2.45,	2.01,	0.87,	0.72,	2004.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-3-snake-eater/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg'),
        (2, 'Metal Gear Solid 4: Guns of the Patriots', 'metal-gear-solid-4-guns-of-the-patriots',	
        'Action', 'M', 'PS3', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 ,0.0,	2.45,	2.01,	0.87,	0.72,	2004.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-4-guns-of-the-patriots/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg')
                ]
    
    expected_data = [
        (1, 'Metal Gear Solid 2: Sons of Liberty', 'metal-gear-solid-2-sons-of-liberty',	
        'Action', 'M', 'PS2', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 ,6.05,	2.45,	2.01,	0.87,	0.72,	2001.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-2-sons-of-liberty/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg')
    ]

    df = spark_session.createDataFrame(actual_data, sales_schema)
    actual_df = drop_global_sales_nulls(df)
    expected_df = spark_session.createDataFrame(expected_data, sales_schema)

    assert actual_df.collect() == expected_df.collect(), 'No se generó el dataframe esperado.'
    

def group_sales_by_game(spark_session):
    sales_schema = StructType([
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
               ])

    actual_data =  [
        (1, 'Metal Gear Solid 2: Sons of Liberty', 'metal-gear-solid-2-sons-of-liberty',	
        'Action', 'M', 'PS2', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 ,6.05,	2.45,	2.01,	0.87,	0.72,	2001.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-2-sons-of-liberty/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg'),
        (1, 'Metal Gear Solid 2: Sons of Liberty', 'metal-gear-solid-2-sons-of-liberty',	
        'Action', 'M', 'XBOX', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 ,4.05,	2.45,	2.01,	0.87,	0.72,	2001.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-2-sons-of-liberty/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg'),
        (1, 'Metal Gear Solid 2: Sons of Liberty', 'metal-gear-solid-2-sons-of-liberty',	
        'Action', 'M', 'PC', 'Konami', 'Konami Computer Entertainment Japan', 9.0 ,9.5, 
        7.0, 80000.0 ,5.05,	2.45,	2.01,	0.87,	0.72,	2001.0,	'Never',	
        'http://www.vgchartz.com/game/3774/metal-gear-solid-2-sons-of-liberty/?region=All',	
        1,	9.0, '/games/boxart/7540650ccc.jpg'),
                ]

    expected_data = [
        ('Metal Gear Solid 2: Sons of Liberty', 15.15, 'Action', 'M', 
        'PS2', 'Konami', 'Konami Computer Entertainment Japan')
    ]

    expected_df = spark_session.createDataFrame(expected_data, sales_schema)
    df = spark_session.createDataFrame(actual_data, sales_schema)
    actual_df = group_sales_by_game(df)
    assert actual_df.collect() == expected_df.collect(), 'No se generó el dataframe esperado.'


def test_add_target_col(spark_session):
    actual_schema = StructType([
                StructField("Name", StringType()),
                StructField("Genre", StringType()),
                StructField("Platform", StringType()),
                StructField("Publisher", StringType()),
                StructField("Developer", StringType()),
                StructField("Global_Sales", FloatType())
               ])
    expected_schema = StructType([
                StructField("Name", StringType()),
                StructField("Genre", StringType()),
                StructField("Platform", StringType()),
                StructField("Publisher", StringType()),
                StructField("Developer", StringType()),
                StructField("Global_Sales", FloatType()),
                StructField("Successful", IntegerType())
               ])

    actual_data =  [
        ('Metal Gear Solid','Action', 'PS1', 
        'Konami', 'Konami Computer Entertainment Japan', 6.0),
         ('Metal Gear Solid 2: Sons of Liberty','Action', 'PS2', 
        'Konami', 'Konami Computer Entertainment Japan', 12.0),
         ('Metal Gear Solid 3: Snake Eater','Action', 'PS2', 
        'Konami', 'Konami Computer Entertainment Japan', 7.0),
         ('Metal Gear Solid 4: Guns of the Patriots','Action', 'PS3', 
        'Konami', 'Konami Computer Entertainment Japan', 40.0),
    ]

    expected_data = [ 
        ('Metal Gear Solid','Action', 'PS1', 
        'Konami', 'Konami Computer Entertainment Japan', 6.0, 0),
        ('Metal Gear Solid 2: Sons of Liberty','Action', 'PS2', 
        'Konami', 'Konami Computer Entertainment Japan', 12.0, 0),
        ('Metal Gear Solid 3: Snake Eater','Action', 'PS2', 
        'Konami', 'Konami Computer Entertainment Japan', 7.0, 0),
        ('Metal Gear Solid 4: Guns of the Patriots','Action', 'PS3', 
        'Konami', 'Konami Computer Entertainment Japan', 40.0, 1)]

    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    df = spark_session.createDataFrame(actual_data, actual_schema)
    actual_df = add_target_col(df, 'Global_Sales')

    assert actual_df.collect() == expected_df.collect(), 'No se generó el dataframe esperado.'

def test_normalize_col(spark_session):
    # Esta prueba falla debido a la precisión del floatType
    actual_schema = StructType([
                StructField("Name", StringType()),
                StructField("Genre", StringType()),
                StructField("Platform", StringType()),
                StructField("Publisher", StringType()),
                StructField("Developer", StringType()),
                StructField("Global_Sales", FloatType())
               ])
    expected_schema = StructType([
                StructField("Name", StringType()),
                StructField("Genre", StringType()),
                StructField("Platform", StringType()),
                StructField("Publisher", StringType()),
                StructField("Developer", StringType()),
                StructField("Global_Sales", FloatType()),
                StructField("Global_Sales_scaled", FloatType())
               ])

    actual_data =  [
        ('Metal Gear Solid','Action', 'PS1', 
        'Konami', 'Konami Computer Entertainment Japan', 6.0),
         ('Metal Gear Solid 2: Sons of Liberty','Action', 'PS2', 
        'Konami', 'Konami Computer Entertainment Japan', 12.0),
         ('Metal Gear Solid 3: Snake Eater','Action', 'PS2', 
        'Konami', 'Konami Computer Entertainment Japan', 7.0),
         ('Metal Gear Solid 4: Guns of the Patriots','Action', 'PS3', 
        'Konami', 'Konami Computer Entertainment Japan', 40.0),
    ]

    df = spark_session.createDataFrame(actual_data, actual_schema)
    actual_df = normalize_col(df, 'Global_Sales')
    
    expected_data = [ 
        ('Metal Gear Solid','Action', 'PS1', 
        'Konami', 'Konami Computer Entertainment Japan', 6.0, 0.0),
        ('Metal Gear Solid 2: Sons of Liberty','Action', 'PS2', 
        'Konami', 'Konami Computer Entertainment Japan', 12.0, 0.1764705926179886),
        ('Metal Gear Solid 3: Snake Eater','Action', 'PS2', 
        'Konami', 'Konami Computer Entertainment Japan', 7.0, 0.02941176471),
        ('Metal Gear Solid 4: Guns of the Patriots','Action', 'PS3', 
        'Konami', 'Konami Computer Entertainment Japan', 40.0, 1.0)]

    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    assert actual_df.collect() == expected_df.collect(), 'No se generó el dataframe esperado.'

def test_join(spark_session):
    df1_schema = StructType([
                StructField("Rank", IntegerType()),
                StructField("Name", StringType()),
                ])

    df2_schema = StructType([
                StructField("title", StringType()),
                StructField("value", IntegerType()),
                ])

    expected_schema = StructType([
                StructField("Rank", IntegerType()),
                StructField("Name", StringType()),
                StructField("value", IntegerType()),
                ])

    df1_data = [
        (1, 'God of War'),
        (2, 'Disco Elysium'),
        (3, 'The Legend of Zelda'),
        (4, 'Yakuza'),
        (5, 'Dark Souls'),
    ]
    df1 = spark_session.createDataFrame(df1_data, df1_schema)

    df2_data = [
        ('The Legend of Zelda', 557),
        ('Pokemon', 446),
        ('Dark Souls', 232),
    ]
    df2 = spark_session.createDataFrame(df2_data, df2_schema)

    actual_df = join_dfs(df1, df2)

    expected_data = [
        (5, 'Dark Souls', 232),
        (3, 'The Legend of Zelda', 557)
    ]

    expected_df = spark_session.createDataFrame(expected_data, expected_schema)
    assert actual_df.collect() == expected_df.collect(), 'No se generó el dataframe esperado.'

