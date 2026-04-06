python -m venv river_env
river_env\Scripts\activate
pip install earthengine-api geopandas google-auth google-auth-oauthlib sqlachemy shapely rasterio
uvicorn main:app --host 0.0.0.0 --port 8000 --reload