import argparse
import urllib, json
import time
def main():
    parser = argparse.ArgumentParser(
        description="Import sample data for similar items by attributes engine")
    parser.add_argument('--max', default='100')
    parser.add_argument('--start', default="60000")
    
    args = parser.parse_args()
    print(args)
    
    total = []

    try:
        for counter in range(0,3000):
            print(counter)
            time.sleep( 0.5 )
            start = counter*10 + int(args.start)
            url = "http://api.yummly.com/v1/api/recipes?_app_id=0f2994db&_app_key=23a12537a3ba4297b359c4fe68168ba9&requirePictures=true&maxResult="+args.max+"&start="+str(start)
            response = urllib.urlopen(url)
            
            
            matches = json.loads(response.read())
            for match in matches["matches"]:
                total.append(match)


            with open('data3.txt', 'w') as outfile:
                json.dump(total, outfile)
    except: 
        with open('data3.txt', 'w') as outfile:
            json.dump(total, outfile)


    
    
    
if __name__ == '__main__':
    main()