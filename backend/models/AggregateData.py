from typing import Dict


class ParcelAggregate:
    def __init__(self, parcel_data: Dict):
        self.min = float(parcel_data['min']['N'])
        self.median = float(parcel_data['median']['N'])
        self.max = float(parcel_data['max']['N'])
        self.mean = float(parcel_data['mean']['N'])

    def __repr__(self):
        return f"ParcelAggregate(min={self.min}, median={self.median}, max={self.max}, mean={self.mean})"

class AggregateData:
    def __init__(self, item: Dict):
        self.pk = item['PK']['S']
        self.sk = item['SK']['S']
        self.mean = float(item['mean']['N'])
        self.min = float(item['min']['N'])
        self.max = float(item['max']['N'])
        self.median = float(item['median']['N'])
        self.parcel_agg = {parcel_id: ParcelAggregate(agg['M'])
                           for parcel_id, agg in item['parcel_agg']['M'].items()}

    def __repr__(self):
        parcel_agg_repr = ", ".join(f"{k}: {v}" for k, v in self.parcel_agg.items())
        return (f"AggregateData(pk={self.pk}, sk={self.sk}, mean={self.mean}, "
                f"min={self.min}, max={self.max}, median={self.median}, "
                f"parcel_agg={{ {parcel_agg_repr} }})")

