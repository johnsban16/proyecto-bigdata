from carga_datos import *

if __name__ == '__main__':
    sesion = iniciaSpark()
    sales_df = load_sales_data(sesion, sales_csv='vg_sales.csv')
    ratings_df = load_ratings_data(sesion, ratings_csv='vg_esrb_ratings.csv')

    sales_df = drop_cols(sales_df)
    sales_df = cast_year_col(sales_df)
    sales_df = drop_global_sales_nulls(sales_df)
    sales_df = group_sales_by_game(sales_df)

    sales_ratings_df = join_dfs(sales_df, ratings_df)
    sales_ratings_scaled_df = normalize_col(sales_ratings_df, 'Global_Sales')
    sales_ratings_scaled_df = add_target_col(sales_ratings_scaled_df, 'Global_Sales_scaled')
    # sales_ratings_scaled_df.select('Name', 'Global_Sales', 'Global_Sales_scaled', 'Successful').where(sales_ratings_scaled_df.Successful == 1).show()
    # sales_ratings_scaled_df.show()

    #write_df_in_db(sales_df, 'sales')
    #write_df_in_db(ratings_df, 'ratings')
    write_df_in_db(sales_ratings_scaled_df, 'sales_ratings')