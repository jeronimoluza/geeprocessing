import pandas as pd
import glob

if __name__ == "__main__":
    files = glob.glob("./outputs/*/*.csv")
    
    result = None
    for file in files:
        df = pd.read_csv(file)
        
        # Set hhid as index
        df = df.set_index('hhid')
        
        if result is None:
            result = df
        else:
            result = result.join(df, how='outer', rsuffix='_new')
            drop_columns = [col for col in result.columns if col.endswith('_new')]
            result = result.drop(columns=drop_columns)
    
    result.to_csv("./outputs/contamination.csv", index=True)