import json, csv, requests, requests_cache, sqlite3, dateutil.parser

DB_FILE = 'data.sqlite'
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS data (data)")
c.execute("CREATE TABLE IF NOT EXISTS data (data)")

req = requests.Session()
requests_cache.install_cache('premierleague_players')

headers = {'User-Agent':'Python script gathering data; contact at: reddit.com/u/hypd09', 'Accept-Encoding': 'gzip', 'Content-Encoding': 'gzip'}

player_ids = set()
with open('player_ids.csv','r') as file:
    reader = csv.reader(file)
    for row in reader:
        player_ids.add(row[0])

link = 'https://www.premierleague.com/players/{0}/player/{1}'

done = []
total = len(player_ids)
for player_id in player_ids:
    try:
        player = {'id':player_id}
        
        site = req.get(link.format(player_id,'stats'),headers=headers).text
        if 'allStatContainer' not in site:
            print('nothing for',player_id)
            continue
        data = {}
        for row in site.split('allStatContainer')[1:]:
            name = row.split('data-stat="')[1].split('"',1)[0]
            stat = row.split('>')[1].split('</',1)[0].strip()
            data[name] = stat
        player['data'] = data

        site = req.get(link.format(player_id,'overview'),headers=headers).text
        player['nationality'] = site.split('playerCountry">')[1].split('</',1)[0]
        dob = site.split('>Date of Birth<')[1].split('info">',1)[1].split('</',1)[0]
        player['dob'] = str(dateutil.parser.parse(dob).date())
        player['height'] = site.split('>Height<')[1].split('info">',1)[1].split('</',1)[0]
        player['weight'] = site.split('>Weight<')[1].split('info">',1)[1].split('</',1)[0]

        c.execute('INSERT INTO data VALUES(?)',[player])
        
        print('Done',player_id)
        done.append(player_id)
        print(len(done),'/',total)
    except IndexError:
        print('Error:',player_id)

c.commit()
c.close()
