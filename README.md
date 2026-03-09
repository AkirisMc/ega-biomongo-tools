# biomongo-tools

biomongo-tools is a Python-based utility designed to manage the information inside of Biomongo. Built as a modular and extensible tool, it simplifies the process of importing, updating, removing, renaming and restoring metadata records in a serialised way.

Biomongo is a MongoDB database (document-oriented) that was created to facilitate the storage and retrieval of metadata derived from various BioTeam initiatives, such as the QC initiative.

<p align="center">
<img src="https://github.com/AkirisMc/ega-biomongo-tools/blob/general_maintenance/diagram.png" width="600">
</p>

## Installation

To install biomongo-tools, follow these steps:

1. Clone this repository to your local machine:

```
git clone https://github.com/EGA-archive/ega-biomongo-tools.git
cd ega-biomongo-tools
```

2. Install the required dependencies:

```
pip install -r requirements.txt
```


## Usage

biomongo-tools should be as easy to use as possible. The only thing you need to do is modify the [conf.py](https://github.com/EGA-archive/ega-biomongo-tools/blob/main/conf.py) file taking into account your needs.

Then, the only command you'll need is:

```
python3 tools.py
```

For more information see [user_manual](https://crgcnag.sharepoint.com/:w:/s/Bioteams/IQBPxgGF2HPhRbMWC2sSdOAaAcYSRrDovupZUwsXNMe8Ynk?e=XBdfQa).
