import polars as pl

def process_dow_jones_data(data_frame:pl.DataFrame):
    """
    Processes data by drop nulls, drop nans, and unique values.
    
    Parameters:
    - data (pandas.DataFrame): The data to be processed.

    Returns:
    - pl.DataFrame: A Polars DataFrame after processing.
    """
    data_frame = data_frame.drop_nans()
    data_frame = data_frame.drop_nulls()
    data_frame = data_frame.unique()
    
    return data_frame.rename({
        '^DJI_Adj_Close': 'DJI_Adj_Close',
        '^DJI_Close': 'DJI_Close',
        '^DJI_High': 'DJI_High',
        '^DJI_Low': 'DJI_Low',
        '^DJI_Open': 'DJI_Open',
        '^DJI_Volume': 'DJI_Volume',
        'Date': 'Date'
    }).select([
        "Date",
        "DJI_Open",
        "DJI_High",
        "DJI_Low",
        "DJI_Close",
        "DJI_Adj_Close",
        "DJI_Volume"
    ])
