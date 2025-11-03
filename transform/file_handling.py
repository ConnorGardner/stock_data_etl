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

def load_file(load_dir, load_fn):
    import pandas as pd
    import os

    load_path = os.path.join(load_dir, load_fn)
    df = pd.read_csv(load_path)
    return df

def main():
    save_file()

if __name__ == '__main__':
    main()