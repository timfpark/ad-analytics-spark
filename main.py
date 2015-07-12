from mappers import frame_correlations_to_impressions_mapper, frame_correlations_to_connections_mapper, keyed_by_user_mapper, tile_pair_mapper, frame_tile_id_mapper
from tile import Tile
from pyspark import SparkConf, SparkContext

def main(sc):
    locationLines = sc.textFile('locations.csv')
    locationParts = locationLines.map(lambda l: l.split(","))

    locations = locationParts.map(lambda l: {
        'user_id': l[0],
        'latitude': float(l[1]),
        'longitude': float(l[2]),
        'timestamp': float(l[3])
    })

    frameLines = sc.textFile('frames.csv')
    frameParts = frameLines.map(lambda l: l.split(","))

    frames = frameParts.map(lambda l: {
        'latitude': float(l[0]),
        'longitude': float(l[1]),
        'ad_id': l[2]
    })

    framesByTileId = frames.map(frame_tile_id_mapper)
    framesGroupedByTileId = framesByTileId.groupByKey()

    userKeyed = locations.map(keyed_by_user_mapper)
    userGrouped = userKeyed.groupByKey()

    tilePairs = userGrouped.flatMap(tile_pair_mapper)
    stage1FramePairs = tilePairs.join(framesGroupedByTileId)
    stage1FramePairsRemapped = stage1FramePairs.map(lambda l: (l[1][0], l[1][1]))
    stage2FramePairs = stage1FramePairsRemapped.join(framesGroupedByTileId)
    frameCorrelations = stage2FramePairs.map(lambda l: (l[1][0], l[1][1]))

    impressions = frameCorrelations.flatMap(frame_correlations_to_impressions_mapper)
    impressionsCounts = impressions.reduceByKey(lambda a,b: a + b)
    invertedImpressionsCounts = impressionsCounts.map(lambda t: (t[1], t[0]))
    sortedImpressionCounts = invertedImpressionsCounts.sortByKey(False)

    sortedImpressionCounts.saveAsTextFile('sortedImpressions')

    connections = frameCorrelations.flatMap(frame_correlations_to_connections_mapper)
    connectionsCount = connections.reduceByKey(lambda a,b: a + b)
    invertedConnectionsCounts = connectionsCount.map(lambda t: (t[1], t[0]))
    sortedConnectionsCounts = invertedConnectionsCounts.sortByKey(False);

    sortedConnectionsCounts.saveAsTextFile('sortedConnections')

if __name__ == "__main__":
    conf = SparkConf().setAppName("day part generator")
    sc = SparkContext(conf=conf)

    main(sc)