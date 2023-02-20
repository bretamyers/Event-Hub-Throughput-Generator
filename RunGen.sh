

python -m venv .genvenv

python -m pip install --upgrade pip 

source .venv/bin/activate

# curl -H "Authorization: token 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" -H "Accept: application/octet-stream" https://github.com/bretamyers/EventHub-Throughput-Generator/archive/master.zip -L -o EventHub-Throughput-Generator.zip

# unzip -o EventHub-Throughput-Generator.zip -d EventHub-Throughput-Generator

python -m pip install -r EventHub-Throughput-Generator/EventHub-Throughput-Generator-main/requirements.txt

python EventHub-Throughput-Generator/EventHub-Throughput-Generator-main/main/BuildBatch.py

# rm -r EventHub-Throughput-Generator.zip

rm -r .genvenv
