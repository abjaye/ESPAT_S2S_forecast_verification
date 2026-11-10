# S2S MPAS verification notebooks

These notebooks turn raw subseasonal-to-seasonal (S2S) hindcast output into
zarr stores, compute climatologies and anomalies, average them into biweekly
periods, and verify the models against ERA5 with
[climpred](https://climpred.readthedocs.io/). They are written to run on
NCAR's Casper through the NCAR JupyterHub, with a Dask cluster of PBS jobs
doing the heavy lifting. They work as-is on NCAR systems only: the data
paths, the PBS cluster and the queue names are NCAR-specific, and running
them elsewhere needs those parts adapted.

## Data

| Data | Where it is |
|---|---|
| Raw hindcast output (MPAS, CESM2, CAM6), read by notebook 1 | NCAR Lustre: `/lustre/desc1/espat/s2s/`. It will be published on GDEX later. |
| Processed zarr stores (raw/anomaly, daily/biweekly) for the models and ERA5, read by the analysis notebooks | Listed in the intake catalog `s2s_data_catalog.yml`, currently at NCAR paths. They will move to GDEX. |
| ERA5 verification data | ERA5 daily data from the NCAR Research Data Archive, regridded to the 1° (360×181) grid. Its processed stores are in the catalog (`OBS_ERA5`). |
| Land mask | `lmask_360x181.nc`, in this folder |

The analysis notebooks read only the catalog and the land mask, so they do
not need notebooks 1–4 to be run first.

## 1. Set up the environment

Create the conda environment from `environment.yml` and register it as a
Jupyter kernel:

```bash
conda env create -f environment.yml
conda activate s2s_mpas
python -m ipykernel install --user --name s2s_mpas --display-name "s2s_mpas"
```

In JupyterHub, open each notebook with the **s2s_mpas** kernel.

Versions are pinned. In particular `zarr` must stay below 3 and `intake` below
2, because the data stores and the catalog use the older formats.

## 2. Change the paths for your account

Several paths point to the original author's directories and project. Before
running anything, update:

| File | What to change |
|---|---|
| `cluster.ipynb` | `PROJECT` (your PBS project code, replacing the `PXXXXXXXX` placeholder) in the settings cell. Everything the cluster writes already goes to `$SCRATCH/dask` for whoever runs it. |
| `1_concatenate_S2S.ipynb` | `filepath` (a scratch folder for file links; defaults to `$SCRATCH/tmp/<var>/`) and `data_out` (where the zarr stores are written) |
| `2_` / `3_` / `4_` notebooks | `zarr_dir` (where the zarr stores are read and written) |
| Analysis notebooks | `plotdir` (where figures are saved when `savefig = "yes"`) |

The processing notebooks (1–4) write to `zarr_dir`, so you need write access
there. The analysis notebooks only read, through the catalog
`s2s_data_catalog.yml` and the land mask `lmask_360x181.nc`, both in this
folder.

## 3. Start the Dask cluster

All notebooks connect to one shared Dask cluster that runs in its own
background process (`dask_cluster.py`), so restarting or closing notebooks,
including `cluster.ipynb`, does not stop it.

1. Open `cluster.ipynb`, change the settings cell if needed (project code,
   queue, number of workers, cores and memory per worker, walltime), and run
   the cells down to the dashboard link. This starts `dask_cluster.py` in the
   background with those settings and submits the worker jobs to PBS. If a
   cluster is already running, it connects to it instead. You don't need to
   edit `dask_cluster.py`.

2. The other notebooks connect with:

   ```python
   from dask_cluster import get_client
   client = get_client()
   ```

   `get_client()` raises an error saying to run `cluster.ipynb` if no cluster
   is running. For a notebook in another folder, first add this folder to the
   path: `import sys; sys.path.append("/path/to/this/folder")`.

3. To watch progress, open the Dask tab in JupyterLab and enter
   `proxy/<port>`, where `<port>` is shown by `client.dashboard_link`.

4. When you are done, run the last cell of `cluster.ipynb` (or
   `client.shutdown()` from any notebook). This stops the cluster and cancels
   the PBS worker jobs. The cluster also stops if you stop your JupyterHub
   server. Check with `qstat -u $USER` that no `dask-worker` jobs are left.

To change the settings of a running cluster, stop it and start it again. The
cluster writes everything to `$SCRATCH/dask`: the scheduler file, the cluster
log `dask_cluster.log` (dashboard link, worker job script and any startup
errors), the PBS output of the worker jobs (`dask_logs/`) and the workers'
spill space. Smaller clusters start faster in the queue.

## 4. Process the data (notebooks 1–4)

Run these in order, once per model (and per variable or data type where
noted). Each notebook has a settings cell near the top: change the settings,
then run all the cells below it.

| Notebook | Reads | Writes | Settings to change per run |
|---|---|---|---|
| `1_concatenate_S2S.ipynb` | Raw netCDF hindcast files | `{MODEL}.raw.daily.geospatial.zarr` | `model`, `modelout`, `grid`, `var` (run once per variable; each run appends) |
| `2_generate_model_climatology.ipynb` | raw daily | `{MODEL}.climatology.daily.geospatial.zarr` (smoothed daily climatology) | `model` |
| `3_generate_member_anomalies.ipynb` | raw daily + climatology | `{MODEL}.anom.daily.geospatial.zarr` | `model` |
| `4_biweekly_averages.ipynb` | raw or anom daily | `{MODEL}.{raw\|anom}.biweekly.geospatial.zarr` | `model`, `data` (`raw` or `anom`); the last cell also builds the ERA5 biweekly file from the ERA5 daily data in the catalog |

Notebook 1 handles the MPAS and CESM/CAM6 runs. The ERA5 daily data were
prepared separately and are read through the catalog.

### Biweekly periods

Notebook 4 averages leads into standard 14-day windows: days 1–14, 15–28 and
29–42, labelled by their first day (leads 1, 15 and 29 = weeks 1–2, 3–4 and
5–6). Models whose names contain two underscores (e.g. `MPAS_60_15km`,
`MPAS_60_4km`) only run 28 days, so they get the first two windows. The ERA5
biweekly value for a date is the mean of that day and the following 13 days,
so it lines up with a forecast window that starts on the same date.

## 5. Analysis notebooks

These read through the catalog and need the cluster running. Each has a
settings cell for the variables, metric, season, years and models. Models are
chosen with a dictionary, `modeln = {"catalog name": "plot label", ...}`
(`metrics_daily_line.ipynb` uses separate `models` and `modeln` lists).

| Notebook | What it makes |
|---|---|
| `metrics_daily_line.ipynb` | Area-averaged ACC, RMSE and spread against daily lead, as line plots |
| `metrics_biweekly_maps.ipynb` | Maps of a metric for each model and biweekly period, plus bar charts of the area averages |

Area averages exclude latitudes south of 60°S. Surface variables (`t2m`, `tp`)
are verified over land only.
