#!/usr/bin/python3
# coding: utf-8

# In[1]:


from pathlib import Path
import json
import time

import requests
from tqdm import tqdm


# In[2]:


base_url = 'https://kudago.com/public-api/v1.4/'
save_path = Path(__file__).expanduser().resolve().parent
save_path.mkdir(parents=True, exist_ok=True)


# In[3]:


actual_since = int(time.time())


# # Места и события

# In[4]:


with save_path.joinpath('events_categories.jsonl').open('w') as f:
    r = requests.get(base_url + 'event-categories', params={'order_by': 'id'})
    for item in tqdm(r.json(), desc='dump events_categories'):
        print(json.dumps(item, ensure_ascii=False), file=f, flush=True)


# In[5]:


with save_path.joinpath('places_categories.jsonl').open('w') as f:
    r = requests.get(base_url + 'place-categories', params={'order_by': 'id'})
    for item in tqdm(r.json(), desc='dump places_categories'):
        print(json.dumps(item, ensure_ascii=False), file=f, flush=True)


# In[6]:


fields = ['id', 'title', 'short_title', 'slug', 'address', 'location', 'timetable', 'phone',
          'is_stub', 'images', 'description', 'body_text', 'site_url', 'foreign_url', 'coords',
          'subway', 'favorites_count', 'comments_count', 'is_closed', 'categories', 'tags']
page = 1
params = {
    'fields': ','.join(fields),
    'page_size': 100,
    'location': 'msk',
    'order_by': 'id'
}
r = requests.get(base_url + 'places', params=params)
with tqdm(total=r.json()['count'], desc='dump places') as pbar:
    with save_path.joinpath('places.jsonl').open('w') as f:
        while True:
            params['page'] = page
            r = requests.get(base_url + 'places', params=params)
            j = r.json()
            for item in j['results']:
                print(json.dumps(item, ensure_ascii=False), file=f, flush=True)
            pbar.update(len(j['results']))
            if j['next']:
                page += 1
            else:
                break


# In[7]:


fields = ['id', 'publication_date', 'dates', 'title', 'short_title', 'slug', 'place', 'description',
          'body_text', 'location', 'categories', 'tagline', 'age_restriction', 'price', 'is_free',
          'images', 'favorites_count', 'comments_count', 'site_url', 'tags', 'participants']

page = 1
params = {
    'fields': ','.join(fields),
    'page_size': 100,
    'location': 'msk',
    'order_by': 'id',
    'expand': 'place,dates,participants',
    'actual_since': actual_since
}
r = requests.get(base_url + 'events', params=params)
with tqdm(total=r.json()['count'], desc='dump events') as pbar:
    with save_path.joinpath('events.jsonl').open('w') as f:
        while True:
            params['page'] = page
            r = requests.get(base_url + 'events', params=params)
            j = r.json()
            for item in j['results']:
                print(json.dumps(item, ensure_ascii=False), file=f, flush=True)
            pbar.update(len(j['results']))
            if j['next']:
                page += 1
            else:
                break


# # Слияние

# In[8]:


data_types = ['places', 'events']
local_id = 0
with save_path.joinpath('places_events.jsonl').open('w') as to_f:
    for data_type in data_types:
        p = save_path.joinpath(f'{data_type}.jsonl')
        with p.open() as from_f:
            for line in tqdm(from_f, desc=f'merging {data_type}'):
                local_id += 1
                j = json.loads(line)
                j['data_type'] = data_type
                j['local_id'] = local_id
                print(json.dumps(j, ensure_ascii=False), file=to_f, flush=True)
        p.unlink()


# # Кино

# In[9]:


fields = ['id', 'site_url', 'publication_date', 'slug', 'title', 'description',
          'body_text', 'is_editors_choice', 'favorites_count', 'genres',
          'comments_count', 'original_title', 'locale', 'country', 'year',
          'language', 'running_time', 'budget_currency', 'budget', 'mpaa_rating',
          'age_restriction', 'stars', 'director', 'writer', 'awards', 'trailer',
          'images', 'poster', 'url', 'imdb_url', 'imdb_rating']

page = 1
params = {
    'fields': ','.join(fields),
    'page_size': 100,
    'location': 'msk',
    'order_by': 'id',
    'actual_since': actual_since
}
r = requests.get(base_url + 'movies', params=params)
with tqdm(total=r.json()['count'], desc='dump movies') as pbar:
    with save_path.joinpath('movies.jsonl').open('w') as f:
        while True:
            params['page'] = page
            r = requests.get(base_url + 'movies', params=params)
            j = r.json()
            for item in j['results']:
                print(json.dumps(item, ensure_ascii=False), file=f, flush=True)
                pbar.update(1)
            if j['next']:
                page += 1
            else:
                break


# In[10]:


mp = save_path.joinpath('movies.jsonl')
with mp.open() as f:
    movies = [json.loads(line) for line in f]
    
with save_path.joinpath('movies_showings.jsonl').open('w') as f:
    for movie in tqdm(movies, desc='dump movies_showings'):
        page = 1
        params = {
            'page_size': 100,
            'location': 'msk',
            'actual_since': actual_since
        }
        movie['showings'] = []
        while True:
            params['page'] = page
            r = requests.get(base_url + f'movies/{movie["id"]}/showings', params=params)
            j = r.json()
            movie['showings'] += j['results']
            if j['next']:
                page += 1
            else:
                break
        print(json.dumps(movie, ensure_ascii=False), file=f, flush=True)

mp.unlink()

