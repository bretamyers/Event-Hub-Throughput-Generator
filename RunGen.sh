
# Create a virtual environment
python -m venv .genvenv

# Activate virtual enviornment
source .genvenv/bin/activate

# Upgrade pip
python -m pip install --upgrade pip 

# Install the required packages
python -m pip install -r requirements.txt

# Run the throughput generator
python main/BuildGen.py

# Deactivate virtual environment
deactivate .genvenv

# Remove the virtual environment
rm -r .genvenv
