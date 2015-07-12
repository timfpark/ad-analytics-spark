import math

class Tile:

    MAX_ZOOM = 16;
    MIN_ZOOM = 1;

    @classmethod
    def tile_id_from_lat_long(cls, latitude, longitude, zoom):
        row = Tile.row_from_latitude(latitude, zoom)
        column = Tile.column_from_longitude(longitude, zoom)

        return Tile.tile_id_from_row_column(row, column, zoom)

    @classmethod
    def row_from_latitude(cls, latitude, zoom):
        return int(
            math.floor(
                (1 - math.log(math.tan(latitude * math.pi / 180) + 1 / math.cos(latitude * math.pi / 180)) / math.pi) / 2 * (2 ** zoom)
            )
        )

    @classmethod
    def column_from_longitude(cls, longitude, zoom):
        return int(
            math.floor(
                (longitude + 180.0) / 360.0 * (2 ** zoom)
            )
        )

    @classmethod
    def latitude_from_row(cls, row, zoom):
        n = math.pi - 2.0 * math.pi * row / (2 ** zoom);
        return (180.0 / math.pi * math.atan(0.5 * (math.exp(n) - math.exp(-n))))

    @classmethod
    def longitude_from_column(cls, column, zoom):
        return float(column) / (2 ** zoom) * 360.0 - 180.0;

    @classmethod
    def tile_from_tile_id(cls, tile_id):
        tile = Tile()
        tile.tile_id = tile_id

        tile.zoom, tile.row, tile.column = Tile.decode_tile_id(tile_id)

        tile.latitude_north = Tile.latitude_from_row(tile.row, tile.zoom)
        tile.latitude_south = Tile.latitude_from_row(tile.row + 1, tile.zoom)

        tile.longitude_west = Tile.longitude_from_column(tile.column, tile.zoom)
        tile.longitude_east = Tile.longitude_from_column(tile.column + 1, tile.zoom)

        tile.center_latitude = (tile.latitude_north + tile.latitude_south) / 2.0
        tile.center_longitude = (tile.longitude_east + tile.longitude_west) / 2.0

        return tile;

    @classmethod
    def decode_tile_id(cls, tile_id):
        parts = tile_id.split('_')
        if len(parts) != 3:
            return

        return int(parts[0]), int(parts[1]), int(parts[2])

    @classmethod
    def tile_id_from_row_column(cls, row, column, zoom):
        return str(zoom) + "_" + str(row) + "_" + str(column)

    @classmethod
    def tile_ids_for_all_zoom_levels(cls, tile_id):
        tile = Tile.tile_from_tile_id(tile_id);

        tile_ids = [];
        for zoom in range(Tile.MIN_ZOOM, Tile.MAX_ZOOM+1):
            tile_id = Tile.tile_id_from_lat_long(tile.center_latitude, tile.center_longitude, zoom);
            tile_ids.append(tile_id);

        return tile_ids;