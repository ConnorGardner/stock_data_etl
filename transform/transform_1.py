from file_handling import save_file, load_file


def load_standardize_df():
    load_fn_base = 'stock_data_v'
    load_dir = '/home/cgardner01/aws_lambda_finnhub/transform/standardize_dfs' #* Keep static
    df = load_file(load_dir=load_dir, load_fn_base=load_fn_base)
    return df

def transformations(df):
    #** Beginning of transformations

    # Values already appear to be sorted, but good practice to sort before doing calculations
    df = df.sort_values(['ticker', 'date'])

    # 1. df['data_series_max'] - signals the new columns to be created
    # 2. df.groupby('ticker') - This splits up the dataframe into groups (almost like mini df's) for each unique ticker.
    # 3. ['high'] - With the dataframe split up by ticker (above), the mini dataframes include the 'high' column along with the tickers.
    # .cummax() - Evaluates the cumulative max from each group. Starts with the first row and works its way down. Given we sorted by date above, this will work how we want.
    #* Reporting the data series max, not to be confused with the high, which is the daily high. Could use a rename in the column
    df['data_series_max'] = df.groupby('ticker')['current_price'].cummax()

    return df

def save_transformed_df(df):
    save_fn_base = 'stock_data_TRANSFORM_v'
    save_path = '/home/cgardner01/aws_lambda_finnhub/transform/t1_dfs'  
    save_file(df=df, save_path=save_path, save_fn_base=save_fn_base)


def main():
    df = load_standardize_df()
    transformed_df = transformations(df)
    save_transformed_df(transformed_df)
    
if __name__ == '__main__':
    main()


