import unittest

from shapely import Polygon


from utils.geomapping import get_geohashes_from_polygon, get_from_max_precision, \
    build_dictionaries_from_crop_assignment, assign_geohashes_to_parcels
from utils.polygon_def import polygon


class TestGeohashAssignment(unittest.TestCase):
    def setUp(self):

        self.filtered_geohashes = get_geohashes_from_polygon(polygon)

        self.list_precision_7_parcels = get_from_max_precision(higher_precision=7, geohashes_list=self.filtered_geohashes)

        # Run your assignment function
        self.crop_assignment = assign_geohashes_to_parcels(
            list_precision_7_parcels=list(self.list_precision_7_parcels),
            list_precision_8_parcels=list(self.filtered_geohashes),
            testing=True
        )
        #print(self.crop_assignment)
        # Build dictionaries from the crop assignment
        self.dictTuple = build_dictionaries_from_crop_assignment(self.crop_assignment)

    def test_unique_geohash8_count(self):
        # Gather all unique geohashes from dictTuple[0] (plant_type_to_geohashes) and dictTuple[1] (geohash6_info)
        unique_geohashes1 = list()
        unique_geohashes2 = list()
        for plant_type, geohashes in self.dictTuple[0].items():
            for gh6, data in geohashes.items():
                unique_geohashes1.extend(data['geohash8'])

        for gh6, info in self.dictTuple[1].items():
            unique_geohashes2.extend(info['geohash8'])

        # Check if the length of filtered_geohashes is the same as the number of unique geohashes
        self.assertEqual(len(self.filtered_geohashes), len(unique_geohashes1),
                         "The number of unique precision 8 geohashes in plant_type_to_geohashes should match the length of filtered_geohashes")
        self.assertEqual(len(self.filtered_geohashes), len(unique_geohashes2),
                         "The number of unique precision 8 geohashes in geohash6_info should match the length of filtered_geohashes")

        # Identify missing geohashes (optional, for additional verification)
        # missing_geohashes1 = set(self.filtered_geohashes) - unique_geohashes1
        # missing_geohashes2 = set(self.filtered_geohashes) - unique_geohashes2
        # if missing_geohashes1:
        #     print("Missing geohashes in plant_type_to_geohashes:", missing_geohashes1)
        # if missing_geohashes2:
        #     print("Missing geohashes in geohash6_info:", missing_geohashes2)
        print(len(self.filtered_geohashes))
        print(len(unique_geohashes2))
        print(len(unique_geohashes1))
        #print(self.dictTuple[0])


if __name__ == '__main__':
    unittest.main()