"""
Import sample data for complementary purchase engine
"""

import predictionio
import argparse
import re
import json


def prepare_data():
    res = []
    with open("data3.txt") as f:
        matches = json.loads(f.read())
        for match in matches["matches"]:
            data = {}
            data["id"] = match["id"]
            data["ingredients"] = match["ingredients"]
            data["name"] = match["recipeName"]
            if 'flavors' in match and match["flavors"] != None and 'smallImageUrls' in match and match["smallImageUrls"] != None:
                data["bitter"] = int(match["flavors"]["bitter"] * 100)
                data["meaty"] = int(match["flavors"]["meaty"] * 100)
                data["piquant"] = int(match["flavors"]["piquant"] * 100)
                data["salty"] = int(match["flavors"]["salty"] * 100)
                data["sour"] = int(match["flavors"]["sour"] * 100)
                data["sweet"] = int(match["flavors"]["sweet"] * 100)
                data["image"] = match["smallImageUrls"][0] + "0"
                res.append(data)


            
    f.close()
    return res
    
                

def import_events(client, data):
    count = 0
    
    for el in data:
        count += 1
        client.create_event(
            event="$set",
            entity_type="item",
            entity_id=el["id"],
            properties=el)

    print("%s events are imported." % count)
            
        
def main():
    parser = argparse.ArgumentParser(
        description="Import sample data for similar items by attributes engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")
    
    args = parser.parse_args()
    print(args)
    
    client = predictionio.EventClient(access_key=args.access_key, url=args.url, 
        threads=4, qsize=100)
    
    data = prepare_data()
    import_events(client, data)
    
    
if __name__ == '__main__':
    main()