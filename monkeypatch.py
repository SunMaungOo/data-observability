import pandas as pd

orginal_read_csv = pd.read_csv

def read_csv_with_data_observability(*args,**kawrgs)->pd.DataFrame:
    df = orginal_read_csv(*args,**kawrgs)
    print("My monkey patch code")
    return df


# notice that we have dynamically inject our read_csv_with_data_observability function as pd.read_csv

pd.read_csv = read_csv_with_data_observability

def main():
    # notice that even though user called pd.read_csv function it actually called read_csv_with_data_observability, because we have inject it
    app_tech = pd.read_csv(
        "data/AppTech.csv",
        parse_dates=["Date"],
        dtype={
            "Symbol":"category"
        })


if __name__=="__main__":
    main()

