import dfm_tools as dfmt
import argparse
import os
import xarray as xr
from dask.distributed import Client
import dask
import numpy as np


def calculate_distance_haversine(lat, lon, lat_or, lon_or):
    '''Function to calculate the shortest distance between two sets of coordinates.
       The coordinates of Hoek van Holland have been previously filled in as a starting point
       for all distance calculations.

       Args:
       - lon_or: Reference Longitude
       - lat_or: Reference Latitude
       - lon: Longitude
       - lat: Latitude

       Returns:
       - distance: Distance [meters]

       '''
    import pandas as pd
    import pyproj

    geodesic = pyproj.Geod(ellps='WGS84')

    if type(lat) and type(lon) == pd.core.series.Series:
        lat_or_list = [lat_or for n in range(len(lat))]
        lon_or_list = [lon_or for n in range(len(lon))]

        fwd_azimuth, back_azimuth, distance = geodesic.inv(lat.tolist(), lon.tolist(), lat_or_list, lon_or_list)

    else:
        fwd_azimuth, back_azimuth, distance = geodesic.inv(lat, lon, lat_or, lon_or)

    #     x = haversine(row.ix[0],row.ix[1],lat_or,lon_or) #starting point; approx. Botlek harbour
    return distance


def calculate_distance_pythagoras(x1, y1, x2, y2):
    '''Function to calculate the shortest distance between two sets of coordinates in a cartesian coordinate system.

       :param x1: starting coordinate (x-direction)
       :param y1: starting coordinate (y-direction)
       :param x2: ending coordinate (x-direction)
       :param y2: ending coordinate (y-direction)

       :returns distance: Distance [meters]
       :rtpye: np.float

    '''

    distance = np.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

    return distance


def build_edge_node_connectivity(uds):
    # > Get fill value, grid name and dimensions
    fill_value = uds.ugrid.grid.fill_value
    gridname = uds.ugrid.grid.name
    dimn_edges = uds.ugrid.grid.edge_dimension

    # > Get coordinate names
    coord_edge_x, coord_edge_y = uds.ugrid.grid.to_dataset().mesh2d.attrs['node_coordinates'].replace('node',
                                                                                                      'edge').split()

    # > Determine dimension name
    dimn_maxen = f'{gridname}_nMax_edge_nodes'

    # > Get connectivity
    edge_nodes = uds.ugrid.grid.edge_node_connectivity

    # > Make into xr.DataArray with correct sizes, dimensions, and coordinates
    edge_node_connectivity = xr.DataArray(data=edge_nodes, dims=[dimn_edges, dimn_maxen],
                                          coords={f'{coord_edge_x}': ([dimn_edges], uds[f'{coord_edge_x}']),
                                                  f'{coord_edge_y}': ([dimn_edges], uds[f'{coord_edge_y}'])},
                                          attrs={'cf_role': 'edge_node_connectivity', 'start_index': 0,
                                                 '_FillValue': fill_value},
                                          name=uds.ugrid.grid.to_dataset().mesh2d.attrs['edge_node_connectivity'])

    return edge_node_connectivity


def calculate_unit_normal_vectors(uds, **kwargs):
    import xugrid as xu

    # > Get dimensions, gridname, and coordinates
    dimn_edges = uds.ugrid.grid.edge_dimension
    dimn_nodes = uds.ugrid.grid.node_dimension
    fill_value = uds.ugrid.grid.fill_value
    gridname = uds.ugrid.grid.name

    varname_unvs = f'{gridname}_unvs'

    # > See if flow area is already in the variables
    if varname_unvs in uds.variables:
        print(f'Unit normal vectors on edges already present Dataset in variable {varname_unvs}.')
        return uds[varname_unvs]

    else:
        # > Get edge coordinate names
        coord_edge_x, coord_edge_y = uds.ugrid.grid.to_dataset().mesh2d.attrs['node_coordinates'].replace('node',
                                                                                                          'edge').split()

        # > Get kwargs
        edge_node_coords = kwargs.get('edge_node_coords')

        # > See if node-edge connectivity is given as a kwarg. If not, reconstruct it
        if 'edge_node_coords' in kwargs:
            pass
        else:
            edge_nodes = build_edge_node_connectivity(uds)

            # > Build the node_coords
            _, _, node_coords = get_all_coordinates(uds)  # just get the node_coords

            # > Get the coordinates of all nodes belonging to an edge
            edge_node_coords = xr.where(edge_nodes != fill_value, node_coords.isel({dimn_nodes: edge_nodes}), np.nan)

        x1 = edge_node_coords[:, 0, 0]
        x2 = edge_node_coords[:, 1, 0]
        y1 = edge_node_coords[:, 0, 1]
        y2 = edge_node_coords[:, 1, 1]
        x = x2 - x1
        y = y2 - y1

        nf = np.dstack([-y, x])

        # > Calculate the norm and divide by the norm
        unv = nf / np.linalg.norm(nf)
        edge_unvs = unv[0]

        # > Put it in xr.DataArray format
        edge_unvs = xr.DataArray(data=edge_unvs, dims=[dimn_edges, f'{gridname}_nCartesian_coords'],
                                 coords={f'{coord_edge_x}': ([dimn_edges], uds[f'{coord_edge_x}']),
                                         f'{coord_edge_y}': ([dimn_edges], uds[f'{coord_edge_y}'])})
        uds[f'{varname_unvs}'] = edge_unvs

        return edge_unvs


def build_face_edge_connectivity(uds):
    # > Get fill value, grid name, and dimensions
    fill_value = uds.ugrid.grid.fill_value
    gridname = uds.ugrid.grid.name
    dimn_faces = uds.ugrid.grid.face_dimension
    dimn_maxfn = uds.ugrid.grid.to_dataset().mesh2d.attrs['max_face_nodes_dimension']

    # > Get voordinate names
    coord_face_x, coord_face_y = uds.ugrid.grid.to_dataset().mesh2d.attrs['node_coordinates'].replace('node',
                                                                                                      'face').split()

    # > Get connectivity
    face_edges = uds.ugrid.grid.face_edge_connectivity

    # > Make into xr.DataArray with correct sizes, dimensions, and coordinates
    face_edge_connectivity = xr.DataArray(face_edges, dims=[dimn_faces, dimn_maxfn],
                                          coords={f'{coord_face_x}': ([dimn_faces], uds[f'{coord_face_x}']),
                                                  f'{coord_face_y}': ([dimn_faces], uds[f'{coord_face_y}'])},
                                          attrs={'cf_role': 'face_edge_connectivity', 'start_index': 0,
                                                 '_FillValue': fill_value}, name=f'{gridname}_face_edges')

    return face_edge_connectivity


def get_all_coordinates(uds):
    # > Get coordinate names
    coord_face_x, coord_face_y = uds.ugrid.grid.to_dataset().mesh2d.attrs['node_coordinates'].replace('node',
                                                                                                      'face').split()
    coord_edge_x, coord_edge_y = uds.ugrid.grid.to_dataset().mesh2d.attrs['node_coordinates'].replace('node',
                                                                                                      'edge').split()
    coord_node_x, coord_node_y = uds.ugrid.grid.to_dataset().mesh2d.attrs['node_coordinates'].split()

    # > Get dimension names
    dimn_faces = uds.ugrid.grid.face_dimension
    dimn_nodes = uds.ugrid.grid.node_dimension
    dimn_edges = uds.ugrid.grid.edge_dimension

    # > Get grid name
    gridname = uds.ugrid.grid.name

    # > Get face coordinates
    face_array = np.c_[uds.mesh2d_face_x, uds.mesh2d_face_y]  # is NOT equal to uds.grid.face_coordinates
    face_coords = xr.DataArray(data=face_array, dims=[dimn_faces, f'{gridname}_nCartesian_coords'],
                               coords={f'{coord_face_x}': ([dimn_faces], uds[f'{coord_face_x}']),
                                       f'{coord_face_y}': ([dimn_faces], uds[f'{coord_face_y}'])},
                               attrs={'units': 'm', 'standard_name': 'projection_x_coordinate, projection_y_coordinate',
                                      'long_name': 'Characteristic coordinates of mesh face',
                                      'bounds': 'mesh2d_face_x_bnd, mesh_face_y_bnd'})

    # > Get edge coordaintes
    edge_array = uds.ugrid.grid.edge_coordinates  # np.c_[uds.mesh2d_edge_x, uds.mesh2d_edge_y]
    edge_coords = xr.DataArray(data=edge_array, dims=[dimn_edges, f'{gridname}_nCartesian_coords'],
                               coords={f'{coord_edge_x}': ([dimn_edges], uds[f'{coord_edge_x}']),
                                       f'{coord_edge_y}': ([dimn_edges], uds[f'{coord_edge_y}'])},
                               attrs={'units': 'm', 'standard_name': 'projection_x_coordinate, projection_y_coordinate',
                                      'long_name': 'Characteristic coordinates of mesh face',
                                      'bounds': 'mesh2d_face_x_bnd, mesh_face_y_bnd'})

    # > Get node coordinates
    node_array = uds.ugrid.grid.node_coordinates  # np.c_[uds.mesh2d_node_x, uds.mesh2d_node_y]
    node_coords = xr.DataArray(data=node_array, dims=[dimn_nodes, f'{gridname}_nCartesian_coords'],
                               coords={f'{coord_node_x}': ([dimn_nodes], uds[f'{coord_node_x}']),
                                       f'{coord_node_y}': ([dimn_nodes], uds[f'{coord_node_y}'])},
                               attrs={'units': 'm', 'standard_name': 'projection_x_coordinate, projection_y_coordinate',
                                      'long_name': 'Characteristic coordinates of mesh node',
                                      'bounds': 'mesh2d_node_x_bnd, mesh_node_y_bnd'})

    return face_coords, edge_coords, node_coords


def build_edge_face_weights(uds):
    # Get dimension names
    dimn_maxfn = uds.ugrid.grid.to_dataset().mesh2d.attrs['max_face_nodes_dimension']
    dimn_faces = uds.ugrid.grid.face_dimension
    dimn_edges = uds.ugrid.grid.edge_dimension
    fill_value = uds.ugrid.grid.fill_value
    gridname = uds.ugrid.grid.name
    dimn_maxef = f'{gridname}_nMax_edge_faces'

    # > Get the edge-face connectivity
    edge_faces = xr.DataArray(uds.ugrid.grid.edge_face_connectivity, dims=(dimn_edges, dimn_maxef))

    # > Get all relevant coordinates
    face_coords, edge_coords, _ = get_all_coordinates(uds)

    # > Fill edge-face-connectivity matrix with face coordinates
    edge_face_coords = xr.where(edge_faces != fill_value, face_coords.isel({dimn_faces: edge_faces}), np.nan)

    # > Get variables for d1 (distance between neighbouring cell faces through edge)
    # > Obtain these from the edge_face_coords dataset
    x0 = edge_face_coords.isel({f'{gridname}_nMax_edge_faces': 0, f'{gridname}_nCartesian_coords': 0})
    x1 = edge_face_coords.isel({f'{gridname}_nMax_edge_faces': 1, f'{gridname}_nCartesian_coords': 0})
    y0 = edge_face_coords.isel({f'{gridname}_nMax_edge_faces': 0, f'{gridname}_nCartesian_coords': 1})
    y1 = edge_face_coords.isel({f'{gridname}_nMax_edge_faces': 1, f'{gridname}_nCartesian_coords': 1})

    d1 = calculate_distance_pythagoras(x0, y0, x1, y1)

    # > Then get variables for d2 (distance from cell face in the first column to edge)
    x2 = edge_coords.isel({f'{gridname}_nCartesian_coords': 0})
    y2 = edge_coords.isel({f'{gridname}_nCartesian_coords': 1})
    d2 = calculate_distance_pythagoras(x0, y0, x2, y2)

    # > Calculate the weights per edge:
    w = d2 / d1

    return w


def compute_divergence(uds, varname, **kwargs):
    # Get dimension names
    dimn_maxfn = uds.ugrid.grid.to_dataset().mesh2d.attrs['max_face_nodes_dimension']
    dimn_faces = uds.ugrid.grid.face_dimension
    dimn_edges = uds.ugrid.grid.edge_dimension
    fill_value = uds.ugrid.grid.fill_value
    gridname = uds.ugrid.grid.name
    dimn_maxef = f'{gridname}_nMax_edge_faces'

    # Check kwargs
    if 'varname_unvs' in kwargs:
        varname_unvs = kwargs['varname_unvs']
    else:
        varname_unvs = f'{gridname}_unvs'

    # > Calculate the unit normal vectors if not in the dataset already
    try:
        unvs = uds[varname_unvs]
    except:
        # > And if not in the kwargs
        try:
            unvs = kwargs['unvs']
        except:
            unvs = calculate_unit_normal_vectors(uds)

    # > Get the edge-face connectivity
    edge_faces = xr.DataArray(uds.ugrid.grid.edge_face_connectivity, dims=(dimn_edges, dimn_maxef))

    # > Get the face-edge connectivity
    face_edges = build_face_edge_connectivity(uds)

    # > Get the unit normal vectors (nf) also in the face-edges matrix
    fe_nfs = xr.where(face_edges != fill_value, unvs.isel({dimn_edges: face_edges}), np.nan)

    # > Determine if we're looking at a velocity value u1 or u0
    # > Because these are vector quantities in the direction of the normal vector,
    # > to get to the final vector, we have to multiply u1/u0 by the normal vector
    # > first. Also, we need to check their sign for every edge.
    if varname == f'{gridname}_u1' or f'{gridname}_u0':

        # > Fill the face-edges matrix with the varname
        edge_var = uds[f'{varname}'].isel({dimn_edges: face_edges})

        # >> We have to determine the sign of the velocity
        # >> Determine whether the u1 value is positive or negative
        # > Get the mesh2d_nFaces numbering of the 0th column in edge_faces (from
        # >  0 -> 1 is positive)
        pos_fe = xr.where(face_edges != fill_value, edge_faces.isel({dimn_maxef: 0}).isel({dimn_edges: face_edges}),
                          fill_value)

        # > If the number of the 0th column in edge_faces == mesh2d_nFaces, then the
        # > direction is already positive in the right direction.
        # > Otherwise, the direction needs to be flipped
        fe_multiplier = xr.where(pos_fe == uds[dimn_faces], 1, -1)
        edge_var = edge_var * fe_multiplier

        # > Multiply by the unit normal vector to get to a vector quantity
        # > With the multiplication we intend to calculate the dot product
        edge_var = edge_var * fe_nfs

    # > If we're not looking at a vector that is already given on the edges,
    # > but at a scalar, calculate the weights for the scalar interpolation
    # > first, if not given in the kwargs.
    else:
        try:
            face_weights = kwargs['face_weights']
        except:
            face_weights = build_edge_face_weights(uds)

        # > Select the varname on faces in the edge-face connectivity matrix
        edge_var = uds[f'{varname}'].isel({dimn_faces: edge_faces})

        # > Calculate the variable on the edges, based on the face_weights
        edge_var = face_weights * edge_var.isel({f'{dimn_maxef}': 0}) + (1 - face_weights) * edge_var.isel(
            {f'{dimn_maxef}': 1})

        # > Get the variables in the face-edges matrix for later multiplication
        # > with the flow area for the Green-Gauss theorem
        edge_var = edge_var.isel({dimn_edges: face_edges})

    # > Fill face_edge matrix with flow area data
    edge_au = uds[f'{gridname}_au'].isel({dimn_edges: face_edges})

    # > Multiply the variable with the edge area (flow area), multiply by the
    # > "flipped boolean" and the unit normal vector, and sum (dimension: faces)
    face_vars = (edge_var * edge_au * fe_nfs).sum(dim=dimn_maxfn, keep_attrs=True)

    # > Multiply the total result with (1/cell volume) (dimension: faces)
    divergence = (1 / uds[f'{gridname}_vol1']) * face_vars
    uds[f'{varname}_div'] = divergence

    return divergence

if __name__ == '__main__':

    # > Set up argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', type=str, required=True)
    parser.add_argument('-o', '--out_file', type=str)  # type=dir_path)
    parser.add_argument('-ml', '--memory_limit', type=str, required=True)
    parser.add_argument('-bc', '--batch_cores', type=str, required=True)
    # parser.add_argument('-d', '--drop_variables', nargs='*', type=str)
    args = parser.parse_args()

    # > Get arguments
    file_nc = args.file
    file_nc = file_nc.replace('_0000_', '_0*_')
    drops = args.drop_variables
    out_file = args.out_file
    mem_lim = args.memory_limit
    batch_cores = args.batch_cores

    n_cores = 4
    n_processes = 4
    n_workers = n_cores * n_processes
    max_mem_alloc = 1.75 * int(batch_cores)  # 1.75 = 336/192
    mem_lim = str(int(np.floor(max_mem_alloc))) + 'GB'  # 336 GiB memory for genoa node

    print('Starting client...')
    client = Client(n_workers=16, threads_per_worker=1, memory_limit="21GB")
    client.amm.start()  # automatic memory management
    print('Started client.')

    # Open the partitioned dataset with xarray
    print('Loading large dataset...')
    # with ProgressBar():
    ds = dfmt.open_partitioned_dataset(file_nc, chunks={'time': 1, 'mesh2d_nFaces': 400000})

    print('Large dataset loaded.')
    times = ds.time
    gridname = ds.ugrid.grid.name

    # > Select only the relevant variables in the new for gradient computation /
    # > salinity variance analysis. {gridname}_node_z is necessary to keep the
    # > nodal dimension.
    # ds = ds[[f'{gridname}_node_z', f'{gridname}_sa1', f'{gridname}_au', f'{gridname}_vol1', f'{gridname}_u1',
    #          f'{gridname}_u0', f'{gridname}_tem1', f'{gridname}_vicwwu']]

    # Define counter for possible subdivision of times in writing
    i = 0

    for t in range(len(times)):

        # > Select the time
        print(f'Starting selection of timestep {t}...')
        tds = ds.isel(time=[t])
        print(f'Loaded the dataset at timestep {t}.')

        # # > Interpolate all of the edge-based variables to face-based
        # # > variables: vicwwu
        # vicwwu_face = dfmt.uda_to_faces(tds[f'{gridname}_vicwwu'])
        # tds[f'{gridname}_vicwwu'] = vicwwu_face
        #
        # # > If i = 0, then it's the first calculation of the script.
        # # > Do all of the things that need to be done only once first.
        # if i == 0:
        #     print('Building unit normal vectors...')
        #     # w = build_edge_face_weights(tds)
        #     unvs = calculate_unit_normal_vectors(tds)
        #     print('Unit normal vectors built and saved.')
        # else:
        #     pass
        #
        # # > Compute the divergence of the u1 variable
        # print(f'Computing the divergence of timestep {t}')
        # divergence_u1 = compute_divergence(tds, f'{gridname}_u1', unvs=unvs)
        # divergence_u1.compute()
        # tds[f'{gridname}_divergence_u1'] = divergence_u1

        if t == 0:
            # > For t = 0 in the range of timesteps, check if there's a file
            # > already present. If it is, remove it.
            if os.path.isfile(out_file):
                os.remove(out_file)

            # Use dask.delayed to write file to disk
            print('Writing file to disk...')
            print(f"Outputfile: {out_file}")
            write_task = dask.delayed(tds.ugrid.to_netcdf(out_file, mode='w'))

        else:
            # Use dask.delayed to write file to disk
            print('Writing file to disk...')
            print(f"Outputfile: {out_file}")
            write_task = dask.delayed(tds.ugrid.to_netcdf(out_file, mode='a'))

        write_task.compute()
        tds.close()

        # > Update counter
        i += 1

    client.close()