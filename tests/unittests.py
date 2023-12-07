import unittest

from shapely import Polygon


from utils.geomapping import get_geohashes_from_polygon, get_from_max_precision, \
    build_dictionaries_from_crop_assignment, assign_geohashes_to_parcels


class TestGeohashAssignment(unittest.TestCase):
    def setUp(self):
        coordinates = [
            (28.1250063, 46.6334964),
            (28.1334177, 46.6175812),
            (28.1556478, 46.6224742),
            (28.1456915, 46.638609),
            (28.1250063, 46.6334964)
        ]

        self.polygon = Polygon(coordinates)
        self.filtered_geohashes = get_geohashes_from_polygon(self.polygon)

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
        # Check for duplicates in crop_assignment
        unique_assignment_geohashes = set(self.crop_assignment.keys())
        self.assertEqual(len(unique_assignment_geohashes), len(self.crop_assignment),
                         "There are duplicate geohashes in crop_assignment.")

        # Gather all unique geohashes from dictTuple[0] and dictTuple[1]
        unique_geohashes1 = set()
        unique_geohashes2 = set()
        for plant_type, geohashes in self.dictTuple[0].items():
            for gh6, gh8_list in geohashes.items():
                unique_geohashes1.update(gh8_list)

        for gh6, info in self.dictTuple[1].items():
            unique_geohashes2.update(info['geohash8'])

        # Check if the length of filtered_geohashes is the same as the number of unique geohashes
        self.assertEqual(len(self.filtered_geohashes), len(unique_geohashes1),
                         f"1. The number of unique precision 8 geohashes in the dictionaries should match the length of filtered_geohashes: {len(self.filtered_geohashes)}")
        self.assertEqual(len(self.filtered_geohashes), len(unique_geohashes2),
                         f"2. The number of unique precision 8 geohashes in the dictionaries should match the length of filtered_geohashes: {len(self.filtered_geohashes)}")

        # Identify missing geohashes
        missing_geohashes = set(self.filtered_geohashes) - unique_geohashes1
        if missing_geohashes:
            print("Missing geohashes:", missing_geohashes)
        print(len(self.filtered_geohashes))
        print(len(unique_geohashes2))
        print(len(unique_geohashes1))


if __name__ == '__main__':
    unittest.main()