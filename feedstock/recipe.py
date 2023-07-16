import fsspec

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

fs = fsspec.filesystem(
    's3', anon=True, client_kwargs={'endpoint_url': 'https://mghp.osn.xsede.org'}
)

all_paths = sorted(fs.glob('s3://rsignellbucket1/LiveOcean/*.nc'))

pattern = pattern_from_file_sequence(['s3://' + path for path in all_paths], 'ocean_time')


recipe = HDFReferenceRecipe(
    pattern,
    netcdf_storage_options={
        'anon': True,
        'client_kwargs': {'endpoint_url': 'https://mghp.osn.xsede.org'},
    },
    identical_dims=['lat_psi', 'lat_rho', 'lat_u', 'lat_v', 'lon_psi', 'lon_rho', 'lon_u', 'lon_v'],
)


import apache_beam as beam
from pangeo_forge_recipes.transforms import OpenWithKerchunk, CombineReferences, WriteCombinedReference

store_name = "cmip6_reference"
transforms = (
    # Create a beam PCollection from our input file pattern
    beam.Create(pattern.items())
    # Open with Kerchunk and create references for each file
    | OpenWithKerchunk(file_type=pattern.file_type, storage_options={'anon':True})
    # Use Kerchunk's `MultiZarrToZarr` functionality to combine the reference files into a single
    # reference file. *Note*: Setting the correct contact_dims and identical_dims is important.
    | CombineReferences(
        concat_dims=["time"], 
        identical_dims=["lat", "lat_bnds", "lon", "lon_bnds", "lev_bnds", "lev"],
        mzz_kwargs = {"remote_protocol": "s3"},
    )
    # Write the combined Kerchunk reference to file.
    | WriteCombinedReference(target_root=target_root, store_name=store_name)
)
