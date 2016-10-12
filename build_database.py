#!/usr/bin/env python
from operator import itemgetter

import os
import urllib
import requests
from collections import Counter, defaultdict
import json
import argparse
import psycopg2
import psycopg2.extras
import mwparserfromhell
from mwparserfromhell.nodes.wikilink import Wikilink
from PIL import Image
from io import BytesIO
import subprocess
import geojson

INFOBOX_PREFIX = 'infobox '
CATEGORY_PREFIX = 'category:'

IMAGE_PATH_EN = 'http://upload.wikimedia.org/wikipedia/en/%s/%s/%s'
IMAGE_PATH_COMMONS = 'http://upload.wikimedia.org/wikipedia/commons/%s/%s/%s'
IMAGE_MARKERS = ['Size of this preview: <a href="',
                 '<div class="fullMedia"><a href="']

def fetch_image(image_name, image_cache):
  if not image_name or image_name.lower().endswith('.tiff') or image_name.lower().endswith('.png'):
    return None
  image_name = image_name.strip('[')
  image_name = image_name.replace(' ', '_')
  if image_name[0].upper() != image_name[0]:
    image_name = image_name.capitalize()
  image_name = urllib.quote(image_name.encode('utf-8'))
  if image_name.startswith('%3C%21--_'):
    image_name = image_name[len('%3C%21--_'):]
  file_path = os.path.join(image_cache, image_name)
  if os.path.isfile(file_path):
    return file_path
  else:
    for prefix in 'http://en.wikipedia.org/wiki/', 'http://en.wikipedia.org/wiki/File:', 'http://commons.wikimedia.org/wiki/', 'http://commons.wikimedia.org/wiki/File:':
      request = requests.get(prefix + image_name)
      if request.status_code == 404:
        continue
      html = request.text
      for marker in IMAGE_MARKERS:
        p = html.find(marker)
        if p == -1:
          continue
        p += len(marker)
        p2 = html.find('"', p)
        url = html[p: p2]
        if url.startswith('//'):
          url = 'http:' + url
        r = requests.get(url)
        if r.status_code != 404:
          try:
            image = Image.open(BytesIO(r.content))
            image.save(file(file_path, 'w'))
          except IOError:
            return None
          return file_path
    print 'no img in html', `image_name`
    return None


def stored_json(file_name):
  def decorator(func):
    def new_func(cursor, json_cache):
      path = os.path.join(json_cache, file_name)
      if os.path.isfile(path):
        res = json.load(file(path))
      else:
        res = func(cursor, json_cache)
        json.dump(res, file(path, 'w'))
      return res
    return new_func
  return decorator


@stored_json('museums.json')
def get_museums(postgres_cursor, json_cache):
  museums = {}
  postgres_cursor.execute("SELECT wikipedia.title as museum_name, wikipedia.wikitext, wikistats.viewcount, wikidata.properties->>'coordinate location' as location "
                          "FROM wikipedia JOIN wikistats ON wikipedia.title = wikistats.title LEFT JOIN wikidata ON wikipedia.title = wikidata.wikipedia_id "
                          "WHERE wikipedia.infobox = 'museum' ORDER BY wikistats.viewcount DESC limit 1000")
  for museum in postgres_cursor:
    museum = dict(museum)
    if not museum.get('location'):
      continue
    wikicode = mwparserfromhell.parse(museum.pop('wikitext'))
    image_file = None
    for template in wikicode.filter_templates():
      if template.name.strip().lower().startswith(INFOBOX_PREFIX):
        for param in template.params:
          if param.name.strip() == 'image_file' or param.name.strip() == 'image':
            image_file = param.value.strip_code().strip()

    location = json.loads(museum.pop('location'))
    museums[museum['museum_name']] = dict(museum, lat=location['lat'], lng=location['lng'], museum_image=image_file)
  return museums


@stored_json('paintings.json')
def get_paintings(postgres_cursor, json_cache):
  postgres_cursor.execute("SELECT wikipedia.*, wikistats.viewcount "
                          "FROM wikipedia JOIN wikistats ON wikipedia.title = wikistats.title "
                          "WHERE general @> ARRAY['paintings'] AND NOT infobox = 'artist' AND NOT infobox = 'person'")
  res = []
  for painting in postgres_cursor:
    name = painting['title']
    wiki_id = name
    wikicode = mwparserfromhell.parse(painting['wikitext'])
    image_file = None
    year = None
    museum = None
    artist = None
    for template in wikicode.filter_templates():
      if template.name.strip().lower().startswith(INFOBOX_PREFIX):
        for param in template.params:
          param_name = param.name.strip()
          if param_name == 'image_file' or param_name == 'image':
            image_file = param.value.strip_code().strip()
          elif param_name == 'name':
            name = param.value.strip_code()
          elif param_name == 'year':
            year = param.value.strip_code()
          elif param_name == 'museum':
            refs = [unicode(node.title) for node in param.value.filter_wikilinks() if isinstance(node, Wikilink)]
            if refs:
              museum = refs[0]
          elif param_name == 'artist':
            artist = param.value.strip_code()

    if image_file and museum:
      res.append({'painting_wiki_id': wiki_id,
                  'painting_name': name,
                  'year': year,
                  'painting': image_file,
                  'museum': museum,
                  'artist': artist,
                  'painting_viewcount': painting['viewcount'],
                  })
  return res



def get_paintings_in_museums(postgres_cursor, json_cache):
  paintings = get_paintings(postgres_cursor, json_cache)
  museums = get_museums(postgres_cursor, json_cache)
  res = []
  for painting in sorted(paintings, key=itemgetter('painting_viewcount'), reverse=True):
    museum = museums.pop(painting['museum'], None)
    if museum:
      painting.update(museum)
      res.append(painting)
  return res


def saved_resized(path, img, scale):
  x, y = map(lambda n: int(n * scale), img.size)
  img = img.resize((x, y), Image.ANTIALIAS)
  img.save(path)


def main(postgres_cursor, json_cache, image_cache, results, neural_style_py):
  mk_abs = lambda p:os.path.join(os.getcwd(), p) if p else None
  paintings_in_museums = get_paintings_in_museums(postgres_cursor, json_cache)
  museums = []
  for painting in paintings_in_museums:
    img_painting_path = mk_abs(fetch_image(painting['painting'], image_cache))
    img_museum_path = mk_abs(fetch_image(painting['museum_image'], image_cache))
    if not img_museum_path or not img_painting_path:
      continue
    target = mk_abs(os.path.join(results, painting['museum'] + '-styled.jpg'))
    museum_image = Image.open(file(img_museum_path))
    painting_image = Image.open(file(img_painting_path))
    size = museum_image.size
    scale = 400.0 / max(size)
    try:
      saved_resized(os.path.join(results, painting['museum'] + '-orig.jpg'), museum_image, scale)
      saved_resized(os.path.join(results, painting['museum'] + '-painting.jpg'), painting_image, scale)
    except IOError:
      continue
    width = int(scale * size[0])
    if not os.path.isfile(target):
      cmd = ['python', neural_style_py,
               '--content', img_museum_path,
               '--styles', img_painting_path,
               '--output', target,
               '--width', str(width)
             ]
      print ' '.join(cmd)
      p = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=neural_style_py.rsplit('/', 1)[0])
      for line in p.stdout:
        print line
      p.wait()
      print p.returncode
    museums.append(painting)
  with file(os.path.join(results, 'museums.js'), 'w') as fout:
    fout.write('museums = \n%s;' % json.dumps(museums, indent=2))

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Collect museums and their paintings')
  parser.add_argument('--postgres', type=str,
                      help='postgres connection string')
  parser.add_argument('--neural_style_py', type=str,
                      help='Where the neural_style.py file lives')
  parser.add_argument('json_cache', type=str,
                      help='Where to store the intermediate json files')
  parser.add_argument('image_cache', type=str,
                      help='Where to store the images')
  parser.add_argument('results', type=str,
                      help='Where to store the final results')

  args = parser.parse_args()

  postgres_conn = psycopg2.connect(args.postgres)
  postgres_cursor = postgres_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

  main(postgres_cursor, args.json_cache, args.image_cache, args.results, args.neural_style_py)




