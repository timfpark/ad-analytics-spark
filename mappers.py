from datetime import datetime
from tile import Tile

VALUE_FIELD = 1

def frame_correlations_to_impressions_mapper(correlation):
    return [(frame, 1) for frame in list(correlation[0])]

def frame_correlations_to_connections_mapper(correlation):
    connections = []

    for fromFrame in list(correlation[0]):
        for toFrame in list(correlation[1]):
            connections.append(
                (fromFrame + '_to_' + toFrame, 1)
            )

    return connections

def remap_rhs_to_kv(tuple):
    return (tuple[VALUE_FIELD][0], tuple[VALUE_FIELD][1])

def keyed_by_user_mapper(location):
    return (location['user_id'], location)

def tile_pair_mapper(location):
    locations = list(location[VALUE_FIELD])
    locations.sort(key=lambda x: x['timestamp'])

    previousTileIds = []
    tilePairs = []
    lastTileId = None

    if len(locations):
        for location in locations:
            tileId = Tile.tile_id_from_lat_long(location['latitude'], location['longitude'], 19)
            if tileId != lastTileId:
                for previousTileId in previousTileIds:
                    tilePairs.append(
                        (previousTileId, tileId)
                    )

                previousTileIds.append(tileId)
                lastTileId = tileId

    return tilePairs

def frame_tile_id_mapper(frame):
    tileId = Tile.tile_id_from_lat_long(frame['latitude'], frame['longitude'], 19)
    return (tileId, frame['ad_id'])