
import tomli

def read_toml_file(FileName:str) -> dict:
    with open(FileName, 'rb') as f:
        config = tomli.load(f)
        # print(json.dumps(config, indent=4))
    return config


