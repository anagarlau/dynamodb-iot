import geohash2
import s2sphere

def latlng_to_s2cell_id(lat, lng, level):
    """Convert latitude and longitude to S2 Cell ID at a specified level."""
    latlng = s2sphere.LatLng.from_degrees(lat, lng)
    cell = s2sphere.CellId.from_lat_lng(latlng).parent(level)
    return cell.id()

# Example usage
lat, lng = 37.7749, -122.4194  # San Francisco coordinates
level = 10  # S2 cell level
s2_cell_id = latlng_to_s2cell_id(lat, lng, level)
print(s2_cell_id)

#['u8k1210q', 'u8k1210r', 'u8k1210s', 'u8k1210t', 'u8k1210u', 'u8k1210v', 'u8k1210w', 'u8k1210x', 'u8k1210y', 'u8k1210z', 'u8k12110', 'u8k12111', 'u8k12112', 'u8k12113', 'u8k12114', 'u8k12115', 'u8k12116', 'u8k12117', 'u8k12118', 'u8k12119', 'u8k1211b', 'u8k1211c', 'u8k1211d', 'u8k1211e', 'u8k1211f', 'u8k1211g', 'u8k1211h', 'u8k1211j', 'u8k1211k', 'u8k1211m', 'u8k1211n', 'u8k1211p', 'u8k1211q', 'u8k1211r', 'u8k1211s', 'u8k1211t', 'u8k1211u', 'u8k1211v', 'u8k1211w', 'u8k1211x', 'u8k1211y', 'u8k1211z', 'u8k12120', 'u8k12121', 'u8k12122', 'u8k12123', 'u8k12124', 'u8k12125', 'u8k12126', 'u8k12127', 'u8k12128', 'u8k12129', 'u8k1212b', 'u8k1212c', 'u8k1212d', 'u8k1212e', 'u8k1212f', 'u8k1212g', 'u8k1212h', 'u8k1212k', 'u8k1212s', 'u8k1212t', 'u8k1212u', 'u8k1212v']
lat, lon, lat_err, lon_err = geohash2.decode_exactly('u8k1210r')
print(lat, lon)
latlng = s2sphere.LatLng.from_degrees(lat, lng)
cell = s2sphere.CellId.from_lat_lng(latlng)
print(cell.id())
