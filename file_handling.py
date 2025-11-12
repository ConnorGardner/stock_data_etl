def save_file(df, save_path, save_fn_base):
    import os
    os.makedirs(save_path, exist_ok=True) 
    i = 1
    while True:
        save_fn = f'{save_fn_base}{str(i).zfill(2)}.csv'        
        full_path = os.path.join(save_path, save_fn)
        if not os.path.exists(full_path):
            break
        i += 1
    df.to_csv(full_path, index=False)
    print(f'File: \'{save_fn}\' successfully saved to {save_path}.')

# This needs to be able to look into a previous directory (in this case standardize_dfs) and find the most recent version to pull.
# Will have to strip the version number. User of the function will be required to enter in the filename base, before the xx following the 'v'


#! This will all have to be changed to accomdate running this in lambda.

def load_file(load_dir, load_fn_base):
    from pathlib import Path
    import pandas as pd
    folder = Path(load_dir)
    files = list(folder.glob(f'{load_fn_base}*.csv'))

    if not files:
        raise FileNotFoundError(f'No files found matching the naming convention of {load_fn_base} in {load_dir}.')

    def extract_version(path):
        name = path.stem
        version_str = name.replace(load_fn_base, '')
        try:
            return int(version_str)
        except ValueError:
            return -1
    
    latest_file = max(files, key=extract_version)
    print(f'Loading latest file: {latest_file.name}')
    return pd.read_csv(latest_file)


def main():
    save_file()

if __name__ == '__main__':
    main()